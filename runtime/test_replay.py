"""Fail-closed validation tests for the embedded replay runtime."""
from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch


def load_replay_module():
    """Load replay.py without requiring its runtime-only boto3 dependency."""
    boto3 = types.ModuleType("boto3")
    botocore = types.ModuleType("botocore")
    botocore_config = types.ModuleType("botocore.config")
    botocore_exceptions = types.ModuleType("botocore.exceptions")
    botocore_config.Config = object
    botocore_exceptions.ClientError = type("ClientError", (Exception,), {})

    modules = {
        "boto3": boto3,
        "botocore": botocore,
        "botocore.config": botocore_config,
        "botocore.exceptions": botocore_exceptions,
    }
    path = Path(__file__).with_name("replay.py")
    spec = importlib.util.spec_from_file_location("floceed_replay_under_test", path)
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, modules):
        spec.loader.exec_module(module)
    return module


replay = load_replay_module()


class ReplayValidationTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        (self.root / "bundle").mkdir()
        self.manifest = {
            "schema_version": 1,
            "source": {"account_id": "123456789012", "region": "us-east-1"},
            "snapshots": [],
        }
        self.write_json("bundle/manifest.json", self.manifest)
        self.write_json("checksums.json", {"schema_version": 1, "files": []})
        self.settings = patch.multiple(
            replay,
            ROOT=self.root,
            ENDPOINT="http://127.0.0.1:4566",
        )
        self.settings.start()

    def tearDown(self):
        self.settings.stop()
        self.tempdir.cleanup()

    def write_json(self, relative: str, value: dict) -> None:
        (self.root / relative).write_text(json.dumps(value), encoding="utf-8")

    def assert_rejected(self, message: str) -> None:
        with self.assertRaisesRegex(RuntimeError, message):
            replay.validate_bundle()

    def test_rejects_unsupported_manifest_schema(self):
        self.manifest["schema_version"] = 4
        self.write_json("bundle/manifest.json", self.manifest)

        self.assert_rejected("manifest schema 4 is unsupported")

    def test_accepts_schema_3_governance_allowlist_without_changing_replay_data(self):
        self.manifest["schema_version"] = 3
        self.manifest["governance"] = {
            "profile": "share-safe",
            "policy_identity": "opaque-policy",
            "rules": [{"rule_id": "rule-001", "action": "omit", "count": "1-9"}],
        }
        self.write_json("bundle/manifest.json", self.manifest)

        manifest = replay.validate_bundle()

        self.assertEqual("share-safe", manifest["governance"]["profile"])

    def test_rejects_unapproved_schema_3_governance_fields(self):
        self.manifest["schema_version"] = 3
        self.manifest["governance"] = {"profile": "safe", "policy_identity": "opaque", "target": "customer.email"}
        self.write_json("bundle/manifest.json", self.manifest)

        self.assert_rejected("governance contains unapproved fields")

    def test_rejects_malformed_schema_3_governance_collections(self):
        self.manifest["schema_version"] = 3
        for field, value in (("rules", None), ("cohorts", {}), ("key_ids", "key-1"), ("algorithms", 1)):
            with self.subTest(field=field):
                self.manifest["governance"] = {
                    "profile": "safe",
                    "policy_identity": "opaque",
                    field: value,
                }
                self.write_json("bundle/manifest.json", self.manifest)
                self.assert_rejected("governance collections must be arrays")

    def test_rejects_invalid_source_account(self):
        for account_id in ("123", "12345678901x"):
            with self.subTest(account_id=account_id):
                self.manifest["source"]["account_id"] = account_id
                self.write_json("bundle/manifest.json", self.manifest)

                self.assert_rejected("source account ID must be exactly 12 digits")

    def test_accepts_versioned_service_contract_fixture(self):
        fixture = Path(__file__).with_name("testdata") / "manifest-contract-v1.json"
        self.write_json("bundle/manifest.json", json.loads(fixture.read_text(encoding="utf-8")))

        manifest = replay.validate_bundle()

        self.assertEqual(["s3", "dynamodb"], [item["service"] for item in manifest["snapshots"]])

    def test_accepts_manifest_v2_chunked_dataset(self):
        snapshot = self.dynamodb_snapshot()
        snapshot["dataset"] = {"format": "dynamodb-ndjson-gzip-v1", "records": 0, "source_bytes": 0, "consistency": "best_effort", "chunks": []}
        self.manifest["schema_version"] = 2
        self.manifest["snapshots"] = [snapshot]
        self.write_json("bundle/manifest.json", self.manifest)

        manifest = replay.validate_bundle()

        self.assertEqual(2, manifest["schema_version"])

    def test_rejects_unsupported_structure_version(self):
        self.manifest["snapshots"] = [self.s3_snapshot()]
        self.manifest["snapshots"][0]["structure_version"] = 2
        self.write_json("bundle/manifest.json", self.manifest)

        self.assert_rejected("structure version 2 is unsupported")

    def test_rejects_invalid_service_structures(self):
        cases = (
            ({**self.s3_snapshot(), "structure": {"name": "assets"}}, "S3 structure requires a region"),
            ({**self.dynamodb_snapshot(), "structure": {"name": "records", "key_schema": [], "billing_mode": "PAY_PER_REQUEST"}}, "requires attribute_definitions"),
            ({**self.sqs_snapshot(), "structure": {"name": "jobs", "arn": "arn:aws:sns:eu-west-1:123456789012:jobs"}}, "sqs structure ARN must match resource identity"),
            ({**self.s3_snapshot(), "service": "unknown", "resource": {"service": "unknown", "id": "assets"}}, "service 'unknown' is unsupported"),
        )
        for snapshot, message in cases:
            with self.subTest(message=message):
                self.manifest["snapshots"] = [snapshot]
                self.write_json("bundle/manifest.json", self.manifest)

                self.assert_rejected(message)

    def test_accepts_minimal_event_targets(self):
        self.manifest["snapshots"] = [self.sqs_snapshot(), self.sns_snapshot()]
        self.write_json("bundle/manifest.json", self.manifest)
        manifest = replay.validate_bundle()
        self.assertEqual(["sqs", "sns"], [snapshot["service"] for snapshot in manifest["snapshots"]])

    def test_fifo_topic_creation_preserves_fifo_attribute(self):
        class FakeSNS:
            def __init__(self):
                self.calls = []

            def create_topic(self, **kwargs):
                self.calls.append(kwargs)
                return {"TopicArn": "arn:aws:sns:eu-west-1:123456789012:events.fifo"}

        client = FakeSNS()
        self.assertEqual("arn:aws:sns:eu-west-1:123456789012:events.fifo", replay.ensure_topic(client, {"name": "events.fifo"}))
        self.assertEqual({"FifoTopic": "true"}, client.calls[0]["Attributes"])

    @staticmethod
    def s3_snapshot() -> dict:
        return {
            "resource": {"service": "s3", "type": "bucket", "id": "assets"},
            "service": "s3",
            "structure_version": 1,
            "structure": {"name": "assets", "region": "eu-west-1"},
        }

    @staticmethod
    def dynamodb_snapshot() -> dict:
        return {
            "resource": {"service": "dynamodb", "type": "table", "id": "records"},
            "service": "dynamodb",
            "structure_version": 1,
            "structure": {
                "name": "records",
                "attribute_definitions": [{"name": "id", "type": "S"}],
                "key_schema": [{"name": "id", "type": "HASH"}],
                "billing_mode": "PAY_PER_REQUEST",
            },
        }

    @staticmethod
    def sqs_snapshot() -> dict:
        return {
            "resource": {"service": "sqs", "type": "queue", "id": "jobs"},
            "service": "sqs",
            "structure_version": 1,
            "structure": {"name": "jobs", "arn": "arn:aws:sqs:eu-west-1:123456789012:jobs"},
        }

    @staticmethod
    def sns_snapshot() -> dict:
        return {
            "resource": {"service": "sns", "type": "topic", "id": "events"},
            "service": "sns",
            "structure_version": 1,
            "structure": {"name": "events", "arn": "arn:aws:sns:eu-west-1:123456789012:events"},
        }

    def test_rejects_non_loopback_or_https_endpoint(self):
        for endpoint in ("http://example.com:4566", "https://localhost:4566"):
            with self.subTest(endpoint=endpoint), patch.object(replay, "ENDPOINT", endpoint):
                self.assert_rejected("endpoint must be an HTTP loopback address")

    def test_rejects_unsafe_bundle_paths(self):
        for relative in ("../outside", str(self.root / "absolute")):
            with self.subTest(relative=relative):
                with self.assertRaisesRegex(RuntimeError, "unsafe bundle path"):
                    replay.safe_path(relative)

    def test_rejects_unsupported_checksum_schema(self):
        self.write_json("checksums.json", {"schema_version": 2, "files": []})

        self.assert_rejected("unsupported checksum schema")

    def test_rejects_checksum_size_or_digest_mismatch(self):
        artifact = self.root / "bundle" / "artifact.json"
        artifact.write_bytes(b"actual")
        actual_digest = hashlib.sha256(b"actual").hexdigest()
        cases = (
            {"path": "bundle/artifact.json", "size": 7, "sha256": actual_digest},
            {"path": "bundle/artifact.json", "size": 6, "sha256": "0" * 64},
        )
        for entry in cases:
            with self.subTest(entry=entry):
                self.write_json("checksums.json", {"schema_version": 1, "files": [entry]})

                self.assert_rejected("checksum mismatch for bundle/artifact.json")

    def test_missing_bucket_uses_captured_bucket_region(self):
        class MissingBucketClient:
            def __init__(self):
                self.create_request = None

            def head_bucket(self, **_kwargs):
                error = replay.ClientError()
                error.response = {"Error": {"Code": "404"}}
                raise error

            def create_bucket(self, **request):
                self.create_request = request

        client = MissingBucketClient()

        replay.ensure_bucket(
            client,
            {"name": "assets", "region": "eu-west-1"},
        )

        self.assertEqual(
            {"LocationConstraint": "eu-west-1"},
            client.create_request["CreateBucketConfiguration"],
        )


if __name__ == "__main__":
    unittest.main()

#!/usr/bin/env python3
"""Idempotent, local-only replay runtime embedded in floceed bundles."""
from __future__ import annotations

import gzip
import base64
import hashlib
import json
import os
import random
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

SUPPORTED_SCHEMA = 1
SUPPORTED_STRUCTURE_VERSION = 1
ROOT = Path(os.environ.get("FLOCEED_ROOT", "/floceed"))
ENDPOINT = os.environ.get("FLOCEED_ENDPOINT", "http://127.0.0.1:4566")


def fail(message: str):
    raise RuntimeError(f"floceed replay: {message}")


def load_json(path: Path):
    with path.open("r", encoding="utf-8") as stream:
        return json.load(stream)


def safe_path(relative: str) -> Path:
    value = Path(relative)
    candidate = (ROOT / value).resolve()
    if value.is_absolute() or ROOT.resolve() not in candidate.parents:
        fail(f"unsafe bundle path {relative!r}")
    return candidate


def validate_bundle() -> dict:
    manifest = load_json(ROOT / "bundle" / "manifest.json")
    version = manifest.get("schema_version")
    if version != SUPPORTED_SCHEMA:
        fail(f"manifest schema {version!r} is unsupported (runtime supports {SUPPORTED_SCHEMA})")
    account = manifest.get("source", {}).get("account_id", "")
    if len(account) != 12 or not account.isdigit():
        fail("source account ID must be exactly 12 digits")
    validate_snapshots(manifest.get("snapshots", []))
    parsed = urlparse(ENDPOINT)
    if parsed.scheme != "http" or parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
        fail("endpoint must be an HTTP loopback address")
    checksums = load_json(ROOT / "checksums.json")
    if checksums.get("schema_version") != 1:
        fail("unsupported checksum schema")
    for entry in checksums.get("files", []):
        digest = hashlib.sha256()
        size = 0
        with safe_path(entry["path"]).open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                size += len(chunk)
                digest.update(chunk)
        if size != entry["size"] or digest.hexdigest() != entry["sha256"]:
            fail(f"checksum mismatch for {entry['path']}")
    return manifest


def validate_snapshots(snapshots: list) -> None:
    if not isinstance(snapshots, list):
        fail("manifest snapshots must be an array")
    for index, snapshot in enumerate(snapshots):
        if not isinstance(snapshot, dict):
            fail(f"snapshot {index} must be an object")
        service = snapshot.get("service")
        resource = snapshot.get("resource")
        if not isinstance(resource, dict) or not service or resource.get("service") != service:
            fail(f"snapshot {index} service must match resource service")
        version = snapshot.get("structure_version")
        if version != SUPPORTED_STRUCTURE_VERSION:
            fail(
                f"snapshot {index} {service} structure version {version!r} is unsupported "
                f"(runtime supports {SUPPORTED_STRUCTURE_VERSION})"
            )
        structure = snapshot.get("structure")
        if not isinstance(structure, dict) or not structure.get("name"):
            fail(f"snapshot {index} {service} structure requires a name")
        if not resource.get("id") or structure["name"] != resource["id"]:
            fail(f"snapshot {index} {service} structure name must match resource ID")
        if service == "s3":
            if not structure.get("region"):
                fail(f"snapshot {index} S3 structure requires a region")
        elif service == "dynamodb":
            if not isinstance(structure.get("attribute_definitions"), list):
                fail(f"snapshot {index} DynamoDB structure requires attribute_definitions")

            if not isinstance(structure.get("key_schema"), list) or not structure["key_schema"]:
                fail(f"snapshot {index} DynamoDB structure requires key_schema")
            if not structure.get("billing_mode"):
                fail(f"snapshot {index} DynamoDB structure requires billing_mode")
        else:
            fail(f"snapshot {index} service {service!r} is unsupported")


def local_client(manifest: dict, service: str):
    source = manifest["source"]
    os.environ["AWS_EC2_METADATA_DISABLED"] = "true"
    options = {"retries": {"mode": "standard", "max_attempts": 3}}
    if service == "s3":
        options["s3"] = {"addressing_style": "path"}
    config = Config(**options)
    return boto3.client(
        service,
        endpoint_url=ENDPOINT,
        region_name=source["region"],
        aws_access_key_id=source["account_id"],
        aws_secret_access_key="test",
        config=config,
    )


def tables(manifest: dict):
    for snapshot in manifest.get("snapshots", []):
        if snapshot.get("service") == "dynamodb":
            yield snapshot["structure"], snapshot


def buckets(manifest: dict):
    for snapshot in manifest.get("snapshots", []):
        if snapshot.get("service") == "s3":
            yield snapshot["structure"], snapshot


def missing(error: ClientError, *codes: str) -> bool:
    return error.response.get("Error", {}).get("Code") in codes


def ensure_bucket(s3, bucket: dict) -> None:
    name = bucket["name"]
    wanted_lock = bool(bucket.get("object_lock", {}).get("ObjectLockConfiguration", {}).get("ObjectLockEnabled") == "Enabled")
    try:
        s3.head_bucket(Bucket=name)
    except ClientError as error:
        if not missing(error, "404", "NoSuchBucket", "NotFound"):
            raise
        request = {"Bucket": name, "ObjectLockEnabledForBucket": wanted_lock}
        if bucket["region"] != "us-east-1":
            request["CreateBucketConfiguration"] = {"LocationConstraint": bucket["region"]}
        s3.create_bucket(**request)
    else:
        try:
            current = s3.get_object_lock_configuration(Bucket=name).get("ObjectLockConfiguration", {})
            current_lock = current.get("ObjectLockEnabled") == "Enabled"
        except ClientError as error:
            if not missing(error, "ObjectLockConfigurationNotFoundError", "NoSuchObjectLockConfiguration"):
                raise
            current_lock = False
        if current_lock != wanted_lock:
            fail(f"existing S3 bucket {name!r} has incompatible immutable object-lock state")


def apply_bucket_mutable(s3, bucket: dict) -> None:
    name = bucket["name"]
    if bucket.get("tags"):
        # PutBucketTagging replaces tags, so merge rather than remove unexpected local tags.
        try:
            current = s3.get_bucket_tagging(Bucket=name).get("TagSet", [])
        except ClientError as error:
            if not missing(error, "NoSuchTagSet"):
                raise
            current = []
        merged = {item["Key"]: item["Value"] for item in current}
        merged.update({item["key"]: item["value"] for item in bucket["tags"]})
        s3.put_bucket_tagging(Bucket=name, Tagging={"TagSet": [{"Key": key, "Value": merged[key]} for key in sorted(merged)]})
    versioning = bucket.get("versioning")
    if versioning in {"Enabled", "Suspended"}:
        s3.put_bucket_versioning(Bucket=name, VersioningConfiguration={"Status": versioning})
    configurations = (
        ("cors", "put_bucket_cors", "CORSConfiguration"),
        ("lifecycle", "put_bucket_lifecycle_configuration", "LifecycleConfiguration"),
        ("encryption", "put_bucket_encryption", "ServerSideEncryptionConfiguration"),
        ("website", "put_bucket_website", "WebsiteConfiguration"),
        ("public_access_block", "put_public_access_block", "PublicAccessBlockConfiguration"),
        ("object_lock", "put_object_lock_configuration", "ObjectLockConfiguration"),
    )
    for field, operation, argument in configurations:
        value = bucket.get(field)
        if not value:
            continue
        # SDK output envelopes generally contain the request structure; website is direct.
        request_value = value.get(argument, value)
        if field == "lifecycle":
            request_value = {"Rules": value.get("Rules", [])}
        getattr(s3, operation)(Bucket=name, **{argument: request_value})


def apply_bucket_links(s3, bucket: dict) -> None:
    name = bucket["name"]
    if bucket.get("policy"):
        s3.put_bucket_policy(Bucket=name, Policy=bucket["policy"])
    notifications = bucket.get("notifications")
    if notifications:
        s3.put_bucket_notification_configuration(Bucket=name, NotificationConfiguration=notifications)


def stream_sha256(body) -> str:
    digest = hashlib.sha256()
    for chunk in iter(lambda: body.read(1024 * 1024), b""):
        digest.update(chunk)
    return digest.hexdigest()


def object_matches(s3, bucket: str, value: dict) -> bool:
    try:
        head = s3.head_object(Bucket=bucket, Key=value["key"], ChecksumMode="ENABLED")
    except ClientError as error:
        if missing(error, "404", "NoSuchKey", "NotFound"):
            return False
        raise
    native = head.get("ChecksumSHA256")
    if native:
        return base64.b64decode(native).hex() == value["sha256"]
    body = s3.get_object(Bucket=bucket, Key=value["key"])["Body"]
    try:
        return stream_sha256(body) == value["sha256"]
    finally:
        body.close()


def seed_bucket(s3, bucket: dict) -> None:
    name = bucket["name"]
    for value in bucket.get("objects", []):
        policy = value.get("overwrite", "if-different")
        if policy == "never":
            try:
                s3.head_object(Bucket=name, Key=value["key"])
            except ClientError as error:
                if not missing(error, "404", "NoSuchKey", "NotFound"):
                    raise
            else:
                continue
        elif policy == "if-different":
            if object_matches(s3, name, value):
                continue
        elif policy != "always":
            fail(f"unsupported S3 overwrite policy {policy!r}")
        request = {"Bucket": name, "Key": value["key"], "Body": safe_path(value["path"]).open("rb")}
        for source, target in (("content_type", "ContentType"), ("content_encoding", "ContentEncoding"), ("cache_control", "CacheControl")):
            if value.get(source):
                request[target] = value[source]
        if value.get("metadata"):
            request["Metadata"] = value["metadata"]
        if value.get("tags"):
            from urllib.parse import urlencode
            request["Tagging"] = urlencode([(item["key"], item["value"]) for item in value["tags"]])
        try:
            s3.put_object(**request)
        finally:
            request["Body"].close()


def keys(values: list[dict]) -> list[dict]:
    return [{"AttributeName": value["name"], "KeyType": value["type"]} for value in values]


def projection(value: dict) -> dict:
    result = {"ProjectionType": value["type"]}
    if value.get("non_key_attributes"):
        result["NonKeyAttributes"] = value["non_key_attributes"]
    return result


def index(value: dict, provisioned: bool) -> dict:
    result = {"IndexName": value["name"], "KeySchema": keys(value["keys"]), "Projection": projection(value["projection"])}
    if provisioned:
        result["ProvisionedThroughput"] = {
            "ReadCapacityUnits": value.get("read_capacity", 1),
            "WriteCapacityUnits": value.get("write_capacity", 1),
        }
    return result


def expected_shape(table: dict) -> dict:
    def projected(value):
        item = value.get("projection", {})
        return (item.get("type"), tuple(sorted(item.get("non_key_attributes", []))))
    def indexes(values):
        return sorted((value["name"], tuple(sorted((key["name"], key["type"]) for key in value["keys"])), projected(value)) for value in values)
    return {
        "attributes": sorted((x["name"], x["type"]) for x in table.get("attribute_definitions", [])),
        "keys": sorted((x["name"], x["type"]) for x in table.get("key_schema", [])),
        "global": indexes(table.get("global_secondary_indexes", [])),
        "local": indexes(table.get("local_secondary_indexes", [])),
    }


def actual_shape(table: dict) -> dict:
    def projected(value):
        item = value.get("Projection", {})
        return (item.get("ProjectionType"), tuple(sorted(item.get("NonKeyAttributes", []))))
    def indexes(name):
        return sorted((value["IndexName"], tuple(sorted((key["AttributeName"], key["KeyType"]) for key in value["KeySchema"])), projected(value)) for value in table.get(name, []))
    return {
        "attributes": sorted((x["AttributeName"], x["AttributeType"]) for x in table.get("AttributeDefinitions", [])),
        "keys": sorted((x["AttributeName"], x["KeyType"]) for x in table.get("KeySchema", [])),
        "global": indexes("GlobalSecondaryIndexes"),
        "local": indexes("LocalSecondaryIndexes"),
    }


def ensure_table(ddb, table: dict) -> None:
    name = table["name"]
    try:
        existing = ddb.describe_table(TableName=name)["Table"]
    except ClientError as error:
        if error.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
        request = {
            "TableName": name,
            "AttributeDefinitions": [{"AttributeName": x["name"], "AttributeType": x["type"]} for x in table["attribute_definitions"]],
            "KeySchema": keys(table["key_schema"]),
            "BillingMode": table.get("billing_mode", "PAY_PER_REQUEST"),
        }
        provisioned = request["BillingMode"] == "PROVISIONED"
        if provisioned:
            request["ProvisionedThroughput"] = {"ReadCapacityUnits": table.get("read_capacity", 1), "WriteCapacityUnits": table.get("write_capacity", 1)}
        if table.get("global_secondary_indexes"):
            request["GlobalSecondaryIndexes"] = [index(x, provisioned) for x in table["global_secondary_indexes"]]
        if table.get("local_secondary_indexes"):
            request["LocalSecondaryIndexes"] = [index(x, False) for x in table["local_secondary_indexes"]]
        stream = table.get("stream", {})
        if stream.get("enabled"):
            request["StreamSpecification"] = {"StreamEnabled": True, "StreamViewType": stream["view_type"]}
        if table.get("tags"):
            request["Tags"] = [{"Key": x["key"], "Value": x["value"]} for x in table["tags"]]
        ddb.create_table(**request)
    else:
        if actual_shape(existing) != expected_shape(table):
            fail(f"existing DynamoDB table {name!r} has an incompatible immutable schema")
    ddb.get_waiter("table_exists").wait(TableName=name, WaiterConfig={"Delay": 1, "MaxAttempts": 60})


def apply_mutable(ddb, table: dict) -> None:
    description = ddb.describe_table(TableName=table["name"])["Table"]
    if table.get("tags"):
        ddb.tag_resource(ResourceArn=description["TableArn"], Tags=[{"Key": x["key"], "Value": x["value"]} for x in table["tags"]])
    ttl = table.get("ttl", {})
    if ttl.get("enabled") and ttl.get("attribute"):
        current = ddb.describe_time_to_live(TableName=table["name"]).get("TimeToLiveDescription", {})
        if current.get("TimeToLiveStatus") not in {"ENABLED", "ENABLING"} or current.get("AttributeName") != ttl["attribute"]:
            ddb.update_time_to_live(TableName=table["name"], TimeToLiveSpecification={"Enabled": True, "AttributeName": ttl["attribute"]})
    wanted = table.get("stream", {})
    current = description.get("StreamSpecification", {})
    if wanted.get("enabled") and (not current.get("StreamEnabled") or current.get("StreamViewType") != wanted.get("view_type")):
        ddb.update_table(TableName=table["name"], StreamSpecification={"StreamEnabled": True, "StreamViewType": wanted["view_type"]})


def batch_write(ddb, table: str, items: list[dict]) -> None:
    pending = [{"PutRequest": {"Item": item}} for item in items]
    for attempt in range(8):
        response = ddb.batch_write_item(RequestItems={table: pending})
        pending = response.get("UnprocessedItems", {}).get(table, [])
        if not pending:
            return
        time.sleep(min(2.0, 0.05 * (2**attempt)) * (0.5 + random.random()))
    fail(f"DynamoDB table {table!r} still has {len(pending)} unprocessed items after bounded retries")


def seed(ddb, snapshot: dict) -> None:
    table = snapshot["structure"]["name"]
    for artifact in snapshot.get("data", []):
        path = safe_path(artifact["path"])
        opener = gzip.open if path.suffix == ".gz" else open
        batch = []
        with opener(path, "rt", encoding="utf-8") as records:
            for line in records:
                if line.strip():
                    batch.append(json.loads(line))
                if len(batch) == 25:
                    batch_write(ddb, table, batch)
                    batch = []
        if batch:
            batch_write(ddb, table, batch)


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1] not in {"base", "links", "data"}:
        fail("usage: replay.py {base|links|data}")
    manifest = validate_bundle()
    ddb = local_client(manifest, "dynamodb")
    s3 = local_client(manifest, "s3")
    if sys.argv[1] == "base":
        for table, _ in tables(manifest):
            ensure_table(ddb, table)
            apply_mutable(ddb, table)
        for bucket, _ in buckets(manifest):
            ensure_bucket(s3, bucket)
            apply_bucket_mutable(s3, bucket)
    elif sys.argv[1] == "links":
        for bucket, _ in buckets(manifest):
            apply_bucket_links(s3, bucket)
    elif sys.argv[1] == "data":
        for _, snapshot in tables(manifest):
            seed(ddb, snapshot)
        for bucket, _ in buckets(manifest):
            seed_bucket(s3, bucket)


if __name__ == "__main__":
    main()

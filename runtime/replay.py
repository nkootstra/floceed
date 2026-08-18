#!/usr/bin/env python3
"""Idempotent, local-only replay runtime embedded in floceed bundles."""
from __future__ import annotations

import gzip
from concurrent.futures import ThreadPoolExecutor, as_completed
import base64
import hashlib
import json
import os
import random
import sys
import tarfile
import time
from pathlib import Path
from urllib.parse import urlparse

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

SUPPORTED_SCHEMAS = {1, 2, 3}
SUPPORTED_STRUCTURE_VERSION = 1
ROOT = Path(os.environ.get("FLOCEED_ROOT", "/floceed"))
ENDPOINT = os.environ.get("FLOCEED_ENDPOINT", "http://127.0.0.1:4566")
REPLAY_WORKERS = max(1, min(32, int(os.environ.get("FLOCEED_REPLAY_WORKERS", "4"))))


def fail(message: str):
    raise RuntimeError(f"floceed replay: {message}")


def progress(phase: str, service: str = "", resource: str = "", completed_records: int = 0, total_records: int = 0, completed_bytes: int = 0, total_bytes: int = 0, precision: str = "") -> None:
    event = {"schema_version": 1, "event": "progress", "operation": "replay", "phase": phase}
    for key, value in (("service", service), ("resource", resource), ("completed_records", completed_records), ("total_records", total_records), ("completed_bytes", completed_bytes), ("total_bytes", total_bytes), ("total_precision", precision)):
        if value:
            event[key] = value
    print("FLOCEED_PROGRESS " + json.dumps(event, separators=(",", ":")), flush=True)


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
    if version not in SUPPORTED_SCHEMAS:
        fail(f"manifest schema {version!r} is unsupported (runtime supports {sorted(SUPPORTED_SCHEMAS)})")
    account = manifest.get("source", {}).get("account_id", "")
    if len(account) != 12 or not account.isdigit():
        fail("source account ID must be exactly 12 digits")
    validate_snapshots(manifest.get("snapshots", []), version)
    validate_governance(manifest.get("governance"), version)
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


def validate_governance(governance: object, manifest_version: int) -> None:
    if governance is None:
        return
    if manifest_version < 3 or not isinstance(governance, dict):
        fail("governance requires manifest schema 3")
    allowed = {"profile", "policy_identity", "cohort_identity", "key_ids", "algorithms", "rules", "cohorts"}
    if set(governance) - allowed:
        fail("governance contains unapproved fields")
    if not isinstance(governance.get("profile"), str) or not governance["profile"] or not isinstance(governance.get("policy_identity"), str) or not governance["policy_identity"]:
        fail("governance profile and policy identity are required")
    rules = governance.get("rules", [])
    cohorts = governance.get("cohorts", [])
    key_ids = governance.get("key_ids", [])
    algorithms = governance.get("algorithms", [])
    if not isinstance(rules, list) or not isinstance(cohorts, list) or not isinstance(key_ids, list) or not isinstance(algorithms, list):
        fail("governance collections must be arrays")
    if not all(isinstance(value, str) and value for value in key_ids + algorithms):
        fail("governance identities must be non-empty strings")
    if len(set(key_ids)) != len(key_ids) or len(set(algorithms)) != len(algorithms):
        fail("governance identities must be unique")
    buckets = {"0", "1-9", "10-99", "100-999", "1000+"}
    rule_ids = set()
    for rule in rules:
        if not isinstance(rule, dict) or set(rule) - {"rule_id", "action", "count"} or not isinstance(rule.get("rule_id"), str) or not rule.get("rule_id") or rule.get("action") not in {"omit", "replace", "hash", "pseudonymize"} or rule.get("count") not in buckets or rule["rule_id"] in rule_ids:
            fail("governance rule audit is invalid")
        rule_ids.add(rule["rule_id"])
    cohort_ids = set()
    for cohort in cohorts:
        if not isinstance(cohort, dict) or set(cohort) - {"resource_identity", "eligible", "retained", "truncated"} or not isinstance(cohort.get("resource_identity"), str) or not cohort.get("resource_identity") or cohort.get("eligible") not in buckets or cohort.get("retained") not in buckets or not isinstance(cohort.get("truncated", False), bool) or cohort["resource_identity"] in cohort_ids:
            fail("governance cohort audit is invalid")
        cohort_ids.add(cohort["resource_identity"])


def validate_snapshots(snapshots: list, manifest_version: int = 1) -> None:
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
        elif service in {"sqs", "sns"}:
            if not isinstance(structure.get("arn"), str) or not structure["arn"]:
                fail(f"snapshot {index} {service} structure requires arn")
            arn = structure["arn"].split(":")
            expected_type = "sqs" if service == "sqs" else "sns"
            if len(arn) != 6 or arn[0] != "arn" or arn[2] != expected_type or arn[5] != resource.get("id"):
                fail(f"snapshot {index} {service} structure ARN must match resource identity")
        elif service == "kinesis":
            if not isinstance(structure.get("arn"), str) or not structure["arn"]:
                fail(f"snapshot {index} Kinesis structure requires arn")
            arn = structure["arn"].split(":")
            if len(arn) != 6 or arn[0] != "arn" or arn[2] != "kinesis" or arn[5] != "stream/" + resource.get("id"):
                fail(f"snapshot {index} Kinesis structure ARN must match resource identity")
        elif service == "events":
            if not isinstance(structure.get("arn"), str) or not structure["arn"]:
                fail(f"snapshot {index} EventBridge structure requires arn")
            arn = structure["arn"].split(":")
            if len(arn) != 6 or arn[0] != "arn" or arn[2] != "events" or arn[5] != "event-bus/" + resource.get("id"):
                fail(f"snapshot {index} EventBridge structure ARN must match resource identity")
        elif service == "lambda":
            if not isinstance(structure.get("arn"), str) or not structure["arn"]:
                fail(f"snapshot {index} Lambda structure requires arn")
            arn = structure["arn"].split(":")
            if len(arn) != 7 or arn[0] != "arn" or arn[2] != "lambda" or arn[5] != "function" or arn[6] != resource.get("id"):
                fail(f"snapshot {index} Lambda structure ARN must match resource identity")
        else:
            fail(f"snapshot {index} service {service!r} is unsupported")
        if manifest_version >= 2:
            if snapshot.get("data"):
                fail(f"snapshot {index} uses legacy data in manifest schema 2")
            dataset = snapshot.get("dataset")
            if dataset:
                formats = {"s3": {"s3-tar-gzip-v1"}, "dynamodb": {"dynamodb-ndjson-v1", "dynamodb-ndjson-gzip-v1"}, "kinesis": {"kinesis-records-ndjson-v1"}, "sqs": {"sqs-messages-ndjson-v1"}}
                if dataset.get("format") not in formats[service] or not isinstance(dataset.get("chunks"), list):
                    fail(f"snapshot {index} has an unsupported dataset")
                records = 0
                source_bytes = 0
                for chunk in dataset["chunks"]:
                    if not isinstance(chunk, dict) or not isinstance(chunk.get("data"), dict) or not chunk["data"].get("path"):
                        fail(f"snapshot {index} has an invalid dataset chunk")
                    if service == "s3" and not isinstance(chunk.get("index"), dict):
                        fail(f"snapshot {index} S3 dataset chunk requires an index")
                    records += chunk.get("records", 0)
                    source_bytes += chunk.get("source_bytes", 0)
                if records != dataset.get("records", 0) or source_bytes != dataset.get("source_bytes", 0):
                    fail(f"snapshot {index} dataset totals do not match chunks")


def local_client(manifest: dict, service: str):
    source = manifest["source"]
    os.environ["AWS_EC2_METADATA_DISABLED"] = "true"
    options = {"retries": {"mode": "standard", "max_attempts": 3}, "request_checksum_calculation": "when_required", "response_checksum_validation": "when_required"}
    if service == "s3":
        options["s3"] = {"addressing_style": "path", "payload_signing_enabled": False}
    config = Config(**options)
    return boto3.client(
        service,
        endpoint_url=ENDPOINT,
        region_name=source["region"],
        aws_access_key_id=source["account_id"],
        aws_secret_access_key="test",
        config=config,
    )


def event_buses(manifest: dict):
    for snapshot in manifest.get("snapshots", []):
        if snapshot.get("service") == "events":
            yield snapshot["structure"], snapshot


def ensure_event_bus(events, bus: dict) -> str:
    name = bus["name"]
    try:
        events.describe_event_bus(Name=name)
    except ClientError as error:
        if not missing(error, "ResourceNotFoundException"):
            raise
        events.create_event_bus(Name=name)
    return bus["arn"]


def apply_event_rules(events, bus: dict) -> None:
    for rule in bus.get("rules", []):
        request = {"Name": rule["name"], "EventBusName": bus["name"], "State": rule.get("state", "ENABLED")}
        if rule.get("description"):
            request["Description"] = rule["description"]
        if rule.get("event_pattern"):
            request["EventPattern"] = rule["event_pattern"]
        events.put_rule(**request)
        targets = [{"Id": target["id"], "Arn": target["arn"], **({"RoleArn": target["role_arn"]} if target.get("role_arn") else {})} for target in rule.get("targets", [])]
        if targets:
            events.put_targets(Rule=rule["name"], EventBusName=bus["name"], Targets=targets)


def tables(manifest: dict):
    for snapshot in manifest.get("snapshots", []):
        if snapshot.get("service") == "dynamodb":
            yield snapshot["structure"], snapshot


def buckets(manifest: dict):
    for snapshot in manifest.get("snapshots", []):
        if snapshot.get("service") == "s3":
            yield snapshot["structure"], snapshot


def targets(manifest: dict):
    for snapshot in manifest.get("snapshots", []):
        if snapshot.get("service") in {"sqs", "sns"}:
            yield snapshot["service"], snapshot["structure"]


def streams(manifest: dict):
    for snapshot in manifest.get("snapshots", []):
        if snapshot.get("service") == "kinesis":
            yield snapshot["structure"], snapshot


def queues(manifest: dict):
    for snapshot in manifest.get("snapshots", []):
        if snapshot.get("service") == "sqs":
            yield snapshot["structure"], snapshot


def topics(manifest: dict):
    for snapshot in manifest.get("snapshots", []):
        if snapshot.get("service") == "sns":
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


def apply_bucket_links(s3, bucket: dict, target_arns: dict[str, str]) -> None:
    name = bucket["name"]
    if bucket.get("policy"):
        s3.put_bucket_policy(Bucket=name, Policy=bucket["policy"])
    notifications = bucket.get("notifications")
    if notifications:
        notifications = json.loads(json.dumps(notifications))
        for field, arn_field, service in (("QueueConfigurations", "QueueArn", "sqs"), ("TopicConfigurations", "TopicArn", "sns")):
            for configuration in notifications.get(field, []):
                captured = configuration.get(arn_field)
                if captured in target_arns:
                    configuration[arn_field] = target_arns[captured]
        s3.put_bucket_notification_configuration(Bucket=name, NotificationConfiguration=notifications)


def ensure_queue(sqs, structure: dict) -> str:
    name = structure["name"]
    attributes = {"FifoQueue": "true"} if name.endswith(".fifo") else {}
    try:
        response = sqs.get_queue_url(QueueName=name)
        url = response["QueueUrl"]
    except ClientError as error:
        if not missing(error, "AWS.SimpleQueueService.NonExistentQueue", "QueueDoesNotExist"):
            raise
        response = sqs.create_queue(QueueName=name, Attributes=attributes)
        url = response["QueueUrl"]
    details = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])
    return details["Attributes"]["QueueArn"]


def ensure_topic(sns, structure: dict) -> str:
    name = structure["name"]
    attributes = {"FifoTopic": "true"} if name.endswith(".fifo") else {}
    return sns.create_topic(Name=name, Attributes=attributes)["TopicArn"]


def apply_topic_links(sns, topic: dict, target_arns: dict[str, str]) -> None:
    topic_arn = target_arns.get(topic.get("arn"), topic.get("arn"))
    for subscription in topic.get("subscriptions", []):
        endpoint = target_arns.get(subscription.get("endpoint"), subscription.get("endpoint"))
        request = {"TopicArn": topic_arn, "Protocol": subscription.get("protocol", "sqs"), "Endpoint": endpoint, "ReturnSubscriptionArn": True}
        sns.subscribe(**request)


def ensure_stream(kinesis, structure: dict) -> None:
    name = structure["name"]
    try:
        kinesis.describe_stream_summary(StreamName=name)
    except ClientError as error:
        if not missing(error, "ResourceNotFoundException"):
            raise
        kinesis.create_stream(StreamName=name, ShardCount=max(1, int(structure.get("shard_count", 1))))
        waiter = kinesis.get_waiter("stream_exists")
        waiter.wait(StreamName=name)


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


def put_object_from_pack(s3, bucket: str, value: dict, body) -> None:
    policy = value.get("overwrite", "if-different")
    if policy == "never":
        try:
            s3.head_object(Bucket=bucket, Key=value["key"])
        except ClientError as error:
            if not missing(error, "404", "NoSuchKey", "NotFound"):
                raise
        else:
            return
    elif policy == "if-different" and object_matches(s3, bucket, value):
        return
    elif policy not in {"always", "if-different"}:
        fail(f"unsupported S3 overwrite policy {policy!r}")
    request = {"Bucket": bucket, "Key": value["key"], "Body": body, "ContentLength": value["size"]}
    for source, target in (("content_type", "ContentType"), ("content_encoding", "ContentEncoding"), ("cache_control", "CacheControl")):
        if value.get(source):
            request[target] = value[source]
    if value.get("metadata"):
        request["Metadata"] = value["metadata"]
    if value.get("tags"):
        from urllib.parse import urlencode
        request["Tagging"] = urlencode([(item["key"], item["value"]) for item in value["tags"]])
    s3.put_object(**request)


def seed_bucket_chunk(s3, bucket: dict, chunk: dict) -> int:
    index_ref = chunk.get("index")
    if not index_ref:
        fail("S3 dataset chunk requires an index")
    completed = 0
    with gzip.open(safe_path(index_ref["path"]), "rt", encoding="utf-8") as index, tarfile.open(safe_path(chunk["data"]["path"]), "r|gz") as archive:
        for line in index:
            if not line.strip():
                continue
            value = json.loads(line)
            member = archive.next()
            if member is None or member.name != value["path"] or not member.isfile():
                fail(f"S3 pack/index mismatch for {value['key']!r}")
            body = archive.extractfile(member)
            if body is None:
                fail(f"S3 pack entry missing for {value['key']!r}")
            put_object_from_pack(s3, bucket["name"], value, body)
            completed += 1
    return completed


def seed_bucket_dataset(s3, bucket: dict, snapshot: dict) -> None:
    dataset = snapshot["dataset"]
    completed = 0
    with ThreadPoolExecutor(max_workers=REPLAY_WORKERS) as executor:
        futures = [executor.submit(seed_bucket_chunk, s3, bucket, chunk) for chunk in dataset.get("chunks", [])]
        for future in as_completed(futures):
            completed += future.result()
            progress("data", "s3", bucket["name"], completed, dataset.get("records", 0), precision="exact")


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


def seed_table_artifact(ddb, table: str, artifact: dict) -> int:
        completed = 0
        path = safe_path(artifact["path"])
        opener = gzip.open if path.suffix == ".gz" else open
        batch = []
        with opener(path, "rt", encoding="utf-8") as records:
            for line in records:
                if line.strip():
                    batch.append(json.loads(line))
                if len(batch) == 25:
                    batch_write(ddb, table, batch)
                    completed += len(batch)
                    batch = []
        if batch:
            batch_write(ddb, table, batch)
            completed += len(batch)
        return completed


def seed(ddb, snapshot: dict) -> None:
    table = snapshot["structure"]["name"]
    dataset = snapshot.get("dataset")
    artifacts = snapshot.get("data", []) if not dataset else [chunk["data"] for chunk in dataset.get("chunks", [])]
    completed = 0
    total = dataset.get("records", 0) if dataset else 0
    with ThreadPoolExecutor(max_workers=REPLAY_WORKERS) as executor:
        futures = [executor.submit(seed_table_artifact, ddb, table, artifact) for artifact in artifacts]
        for future in as_completed(futures):
            completed += future.result()
            progress("data", "dynamodb", table, completed, total, precision="exact" if dataset else "unknown")


def seed_stream(kinesis, snapshot: dict) -> None:
    stream = snapshot["structure"]["name"]
    dataset = snapshot.get("dataset") or {}
    completed = 0
    total = dataset.get("records", 0)
    for chunk in dataset.get("chunks", []):
        with safe_path(chunk["data"]["path"]).open("r", encoding="utf-8") as source:
            batch = []
            for line in source:
                value = json.loads(line)
                batch.append({"Data": base64.b64decode(value["data_base64"]), "PartitionKey": value["partition_key"]})
                if len(batch) == 500:
                    kinesis.put_records(StreamName=stream, Records=batch)
                    completed += len(batch)
                    progress("data", "kinesis", stream, completed, total, precision="exact")
                    batch = []
            if batch:
                kinesis.put_records(StreamName=stream, Records=batch)
                completed += len(batch)
                progress("data", "kinesis", stream, completed, total, precision="exact")


def seed_queue(sqs, snapshot: dict) -> None:
    queue = snapshot["structure"]["name"]
    dataset = snapshot.get("dataset") or {}
    url = sqs.get_queue_url(QueueName=queue)["QueueUrl"]
    completed = 0
    total = dataset.get("records", 0)
    for chunk in dataset.get("chunks", []):
        with safe_path(chunk["data"]["path"]).open("r", encoding="utf-8") as source:
            batch = []
            for line in source:
                value = json.loads(line)
                batch.append({"MessageBody": base64.b64decode(value["body_base64"]).decode("utf-8")})
                if len(batch) == 10:
                    for request in batch:
                        sqs.send_message(QueueUrl=url, **request)
                    completed += len(batch)
                    progress("data", "sqs", queue, completed, total, precision="exact")
                    batch = []
            for request in batch:
                sqs.send_message(QueueUrl=url, **request)
            completed += len(batch)
            if batch:
                progress("data", "sqs", queue, completed, total, precision="exact")


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1] not in {"all", "base", "links", "data"}:
        fail("usage: replay.py {all|base|links|data}")
    manifest = validate_bundle()
    ddb = local_client(manifest, "dynamodb")
    s3 = local_client(manifest, "s3")
    sqs = local_client(manifest, "sqs")
    sns = local_client(manifest, "sns")
    kinesis = local_client(manifest, "kinesis")
    events = local_client(manifest, "events")
    target_arns = {}
    stages = {"base", "links", "data"} if sys.argv[1] == "all" else {sys.argv[1]}
    if "base" in stages:
        progress("base")
        for service, structure in targets(manifest):
            target_arns[structure["arn"]] = ensure_queue(sqs, structure) if service == "sqs" else ensure_topic(sns, structure)
        for topic, _ in topics(manifest):
            apply_topic_links(sns, topic, target_arns)
        for table, _ in tables(manifest):
            ensure_table(ddb, table)
            apply_mutable(ddb, table)
        for bucket, _ in buckets(manifest):
            ensure_bucket(s3, bucket)
            apply_bucket_mutable(s3, bucket)
        for stream, _ in streams(manifest):
            ensure_stream(kinesis, stream)
        for bus, _ in event_buses(manifest):
            ensure_event_bus(events, bus)
            apply_event_rules(events, bus)
    if "links" in stages:
        progress("links")
        for bucket, _ in buckets(manifest):
            for service, structure in targets(manifest):
                if structure["arn"] not in target_arns:
                    target_arns[structure["arn"]] = ensure_queue(sqs, structure) if service == "sqs" else ensure_topic(sns, structure)
            apply_bucket_links(s3, bucket, target_arns)
    if "data" in stages:
        progress("data")
        for _, snapshot in tables(manifest):
            seed(ddb, snapshot)
        for bucket, snapshot in buckets(manifest):
            if snapshot.get("dataset"):
                seed_bucket_dataset(s3, bucket, snapshot)
            else:
                seed_bucket(s3, bucket)
        for _, snapshot in streams(manifest):
            if snapshot.get("dataset"):
                seed_stream(kinesis, snapshot)
        for _, snapshot in queues(manifest):
            if snapshot.get("dataset"):
                seed_queue(sqs, snapshot)


if __name__ == "__main__":
    main()

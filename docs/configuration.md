# Project configuration reference

`floceed.yaml` is the human-owned project definition. Create a safe minimal
file with:

```bash
floceed init --region eu-west-1
```

The command is offline and does not discover or select AWS resources. It writes
`./floceed.yaml`, refuses to overwrite an existing file, and accepts
`--force` only when replacement is intentional. Every project command defaults
to this path; use `--project path/to/floceed.yaml` to select another file.

## Minimal project

```yaml
schema_version: 1
source:
  region: eu-west-1
target:
  floci_version: 1.6.0
  port: 4566
  hook_timeout_seconds: 300
  replay_workers: 4
capture:
  resource_workers: 2
output:
  directory: .floceed
resources: {}
```

`schema_version` is currently `1`. `source.region` is required. `profile` and
`expected_account_id` are optional; the latter must be a 12-digit account ID.
Target, capture, and output values shown above are the defaults applied when
omitted.

## Common commands

```bash
floceed doctor --project floceed.yaml
floceed plan --project floceed.yaml
floceed pull --project floceed.yaml --yes
floceed inspect --project floceed.yaml
floceed render --project floceed.yaml
floceed up --project floceed.yaml
```

`doctor` validates the project and local prerequisites; it does not create a
missing project. Run `floceed init --region REGION` first.

## Top-level fields

| Field | Meaning |
|---|---|
| `source.profile` | AWS SDK profile name; omitted uses the standard credential chain. |
| `source.region` | AWS region to read. Required. |
| `source.expected_account_id` | Optional account pin preventing capture from the wrong account. |
| `target.floci_version` | Floci compatibility version; currently `1.6.0`. |
| `target.port` | Local Floci port; default `4566`. |
| `target.hook_timeout_seconds` | Replay readiness timeout; full captures commonly need more than the default `300`. |
| `target.replay_workers` | Local replay worker bound; default `4`. |
| `target.persistence.enabled` | Whether generated Compose uses persistent Floci state. |
| `target.persistence.volume` / `path` | Named volume or host path for persistence. |
| `capture.allow_partial_data` | Whether bounded capture may complete with explicitly reported partial data. |
| `capture.resource_workers` | Capture concurrency; default `2`. |
| `output.directory` | Generated bundle and Compose directory; default `.floceed`. |

## Resource selections

Each resource entry requires a `name`; services whose AWS APIs need it also
require an `arn`. Names and ARNs must describe the same resource. Duplicate
names within a service are rejected.

| YAML key | Resource identity | Data support |
|---|---|---|
| `s3` | bucket name | `structure`, `bounded`, `full` |
| `dynamodb` | table name | `structure`, `bounded`, `full` |
| `sqs` | queue name and ARN | `structure`, `bounded` |
| `sns` | topic name and ARN | structure only |
| `kinesis` | stream name and ARN | `structure`, `bounded`, `full` |
| `eventbridge` | event bus name and ARN | structure only |
| `lambda` | function name and ARN | structure only |
| `secrets` | secret name and ARN | metadata only; values are never read |
| `parameters` | parameter name and ARN | metadata only; values are never read |
| `api_gateway` | API name and API Gateway v2 ARN | structure only |
| `step_functions` | state-machine name and ARN | structure only |
| `cloudwatch_logs` | log-group name and ARN | structure only |

Structure-only and metadata-only services do not fetch historical payloads.
They preserve topology and compatibility information; they do not imply that
Floci will recreate live traffic or executions.

## Data policies

S3, DynamoDB, and Kinesis accept a `data` block with `enabled` and a `mode` of
`bounded` or `full`. SQS supports `enabled` with `bounded` mode. Bounded policies should set service-specific
limits such as `max_objects`, `max_items`, `max_records`, or `max_messages`.
Full mode removes those bounded limits and requires an intentionally increased
`target.hook_timeout_seconds`.

S3 additionally supports `prefixes`, `max_object_bytes`, `max_total_bytes`,
and `overwrite` (`if-different`, `always`, or `never`). DynamoDB supports
`preserve_provisioned`, `max_items`, `max_pages`, and `gzip`. Kinesis supports
`max_records` and `max_bytes`; SQS supports `max_messages` and `max_bytes`.

## Structure-only examples

```yaml
resources:
  api_gateway:
    - name: orders-api
      arn: arn:aws:apigateway:eu-west-1::/apis/api-id
  step_functions:
    - name: orders
      arn: arn:aws:states:eu-west-1:123456789012:stateMachine:orders
  cloudwatch_logs:
    - name: /app/orders
      arn: arn:aws:logs:eu-west-1:123456789012:log-group:/app/orders:*
```

API Gateway captures API metadata, routes, and integrations, but not requests,
deployments, stages, domains, authorizers, or exports. Step Functions captures
state-machine metadata, logging/tracing settings, and tags, but not definitions,
executions, inputs, outputs, or execution history. CloudWatch Logs captures log
group retention/class/size metadata and tags, but not log events, subscriptions,
metric filters, or historical data.

## Governance profiles

`fixture_profiles` contains named omit, replace, hash, pseudonymize, and cohort
rules. Select a profile explicitly with `--fixture-profile`. Pseudonymization
and cohorts require `FLOCEED_GOVERNANCE_SECRET` containing at least 32 random
bytes encoded as base64. The secret is never written to YAML, bundles, logs,
or checkpoints. Review uncovered fields before sharing a generated fixture.

See [the basic example](../examples/basic/floceed.yaml), [bundle format](bundle-format.md),
and [source IAM policy](iam-policy.md) for complete working context.

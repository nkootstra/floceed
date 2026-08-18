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

Multiple resources of the same service are expressed as additional list items:

```yaml
resources:
  s3:
    - name: orders-assets
      data:
        enabled: true
        mode: bounded
        prefixes: [fixtures/]
        max_objects: 100
    - name: payments-assets
      # Omit data for structure-only bucket metadata.
```

The same list-item pattern applies to every resource key. S3 bucket entries
use bucket names rather than ARNs; Floceed resolves the bucket region and
reads only the selected bucket's configuration and permitted object data.

## Data policies

Omit `data` to capture structure and service metadata only. When a `data` block
is present, `enabled: true` requests payload capture; `enabled: false` keeps the
resource selection but disables payload capture. `mode` defaults to `bounded`.
Bounded mode requires positive service-specific limits. Full mode removes those
bounded limits and is available only for S3, DynamoDB, and Kinesis. Any full-mode
project must set `target.hook_timeout_seconds` above the default because replay
and readiness can take substantially longer.

| Service | `data` fields | Meaning and constraints |
|---|---|---|
| S3 | `enabled`, `mode`, `prefixes`, `max_objects`, `max_object_bytes`, `max_total_bytes`, `overwrite` | `prefixes` filters object keys. In bounded mode all three limits are required. `overwrite` is `if-different` (default), `always`, or `never`. In full mode omit the three bounded limits; prefixes and overwrite still apply. |
| DynamoDB | `enabled`, `mode`, `max_items`, `max_pages`, `gzip` | Bounded mode requires positive item and page limits. Full mode omits `max_items` and `max_pages`. `gzip` controls dataset compression and defaults to enabled. `preserve_provisioned` is a resource field, not part of `data`, and preserves the table's provisioned capacity when true. |
| Kinesis | `enabled`, `mode`, `max_records`, `max_bytes` | Bounded mode uses both limits. Full mode omits them. Records are captured in deterministic shard order; this does not replay live stream traffic. |
| SQS | `enabled`, `mode`, `max_messages`, `max_bytes` | Only `bounded` mode is supported. The limits bound the number and total payload size of messages read; historical messages are not a durable queue history and are not replayed as live traffic. |
| SNS | no `data` fields | Structure-only: topic configuration, subscriptions, and compatibility metadata are captured; published message history is not read. |
| EventBridge | no `data` fields | Structure-only: event-bus/rule topology and compatibility metadata are captured; event history is not read. |
| Lambda | no `data` fields | Structure-only: function configuration and compatibility metadata are captured; invocation payloads and logs are not read. |
| Secrets Manager | no `data` fields | Metadata-only. Secret values are never read. |
| SSM Parameter Store | `with_decryption` on the resource | Metadata-only. Parameter values are never read; `with_decryption` is retained for configuration compatibility but does not authorize or enable value capture. |
| API Gateway | no `data` fields | Structure-only: API metadata, routes, and integrations are captured; requests, deployments, stages, domains, authorizers, and exports are not. |
| Step Functions | no `data` fields | Structure-only: state-machine metadata, logging/tracing settings, and tags are captured; definitions, executions, inputs, outputs, and history are not. |
| CloudWatch Logs | no `data` fields | Structure-only: log-group retention/class/size metadata and tags are captured; log events, subscriptions, metric filters, and historical data are not. |

For example, a bounded S3 policy with every S3-specific field is:

```yaml
data:
  enabled: true
  mode: bounded
  prefixes:
    - fixtures/
  max_objects: 100
  max_object_bytes: 10485760
  max_total_bytes: 104857600
  overwrite: if-different
```

The equivalent full-mode policies deliberately omit bounded limits:

```yaml
resources:
  dynamodb:
    - name: staging-records
      preserve_provisioned: true
      data:
        enabled: true
        mode: full
        gzip: true
  kinesis:
    - name: staging-events
      arn: arn:aws:kinesis:eu-west-1:123456789012:stream/staging-events
      data:
        enabled: true
        mode: full
```

For SQS, keep the policy bounded:

```yaml
resources:
  sqs:
    - name: staging-events
      arn: arn:aws:sqs:eu-west-1:123456789012:staging-events
      data:
        enabled: true
        mode: bounded
        max_messages: 100
        max_bytes: 16777216
```

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

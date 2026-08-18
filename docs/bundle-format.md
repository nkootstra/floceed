# Generated bundle

`floceed.yaml` is human-owned. `.floceed/` is renderer-owned and replaced only
after a staged bundle passes schema, path, checksum, credential-leak, and
Docker Compose validation.

```text
.floceed/
├── compose.generated.yaml
├── init/ready.d/
│   └── 10-replay.py
├── runtime/replay.py
├── bundle/
│   ├── manifest.json
│   └── data/{s3,dynamodb}/
├── checksums.json
└── .gitignore
```

The manifest schema is versioned independently of the project YAML. New pulls
write manifest schema v3; the renderer and embedded runtime continue to accept
v1 and v2. Schema v2 stores DynamoDB data as bounded sorted NDJSON chunks and
S3 data as bounded tar/gzip packs with streamed per-pack indexes, avoiding one
manifest entry and one host file per S3 object. Paths in the manifest are
relative and must remain below `bundle/data/`. The replay
runtime rejects a newer schema, a non-loopback endpoint, an invalid source
account ID, unsafe paths, and changed checksummed files before applying the
stage.

Schema v3 leaves those dataset formats unchanged and adds an optional
`governance` audit object for a selected fixture profile. Its disclosure surface
is deliberately limited to the profile and opaque policy/cohort identities,
non-secret key IDs, algorithm versions, opaque rule/resource identities,
actions, truncation flags, and bucketed counts (`0`, `1-9`, `10-99`,
`100-999`, `1000+`). Exact counts, rule target paths, source values, runtime
secrets and verifiers, and sensitive cohort-rank digests are not serialized.
Schema v1 and v2 manifests must not contain governance metadata.

Fixture transformations happen during capture, before durable artifacts are
written. Replay consumes already-transformed DynamoDB NDJSON and S3 tar/index
artifacts; it does not possess the governance secret and does not transform data
a second time. Changing the effective policy, cohort configuration, algorithm,
key ID, content-type allowlist, or injected secret changes checkpoint identity
and prevents an incompatible resume.

The generated replay hook validates checksums once, then applies base resources,
cross-resource links, and fixture data in that order. Replay creates or updates
selected state but never removes unexpected local resources. Tags are merged.
An incompatible immutable DynamoDB schema or S3 object-lock setting is a hard
failure rather than a delete-and-recreate operation.

SQS and SNS snapshots are minimal identity records (`name` and captured `arn`).
Replay creates selected queues and topics during the base stage, records their
local ARNs, and substitutes those ARNs only in typed S3 queue/topic notification
fields during the links stage. Notification events and key filters are retained;
messages, subscriptions, policies, and delivery settings are outside the bundle
contract. FIFO queue/topic names retain their FIFO creation attributes.

## Integrity, inspection, and semantic receipts

`floceed inspect` reads this directory without AWS or Docker. It first validates
the supported manifest schema, required regular files, safe relative paths, and
the complete `checksums.json` index. A checksum failure is an integrity failure;
Floceed does not describe that bundle as valid.

After integrity validation, Floceed projects the manifest into a canonical,
versioned semantic identity. This projection excludes volatile capture time and
tool version, but retains source scope, target compatibility, selection,
resource structure digests, dataset format/count/size/chunk identities,
operations, findings, and disclosure-safe governance audit semantics. The
semantic digest answers whether replay-relevant meaning changed. It is not a
content checksum and does not replace the byte-integrity index.

Comparisons are sorted by service, type, and resource ID. A resource outcome is
exactly `added`, `removed`, `changed`, or `unchanged`. A changed result carries
only the applicable categories: `structure`, `dataset`, `governance`,
`operations`, `findings`, `selection`, `source`, and `target`. Receipts contain
identities, digests, counts, formats, and safe audit summaries—not old/new
structures, object or record values, salts, replacements, or audit samples.

Schema-3 governance visibility remains limited to opaque policy/cohort/rule and
resource identities, non-secret key IDs, algorithm versions, actions,
truncation flags, and the fixed count buckets documented above. Schemas 1 and 2
produce the same inspection read model without governance fields.

`inspect --runtime` is an explicit, bounded readiness query. If Floci cannot be
reached, runtime state is `unavailable` and the artifact remains valid. Default
inspection and all comparisons are offline and read-only. Inspection does not
compare artifacts with running resources, reconcile drift, or delete anything.

## Capture reuse does not change the bundle

The v0.4 completed-capture ledger is an input-side, runner-local optimization.
Reused artifacts are verified and materialized beside freshly captured
artifacts before rendering. The renderer still stages, validates, and atomically
installs one complete flat directory. Moving or deleting the work directory and
ledger after a successful pull does not affect validation, `floceed up`, or
replay.

Reuse does not change `bundle/manifest.json`, dataset paths or formats,
`checksums.json`, the generated replay hook, or replay ordering. None of those
files contains a ledger path or requires a base bundle. Schemas 1, 2, and 3
retain their existing meanings. Capture-unit outcomes (`reused`, `refreshed`,
and `invalidated`) are receipt metadata beneath the existing resource outcomes;
they are not manifest fields.

Floceed does not produce layered/delta bundles or replay tombstones, and it does
not destructively synchronize source deletions into Floci. A missing source unit
can be reported as `source_unit_missing` and omitted from the newly assembled
standalone bundle; replay continues to use the same non-destructive application
contract described above. Failed capture or rendering preserves the previously
installed, replayable directory.

## CI fixture verification and admission

`floceed fixture verify --input <directory> --output json` validates a local
fixture without loading AWS configuration or contacting a source. It checks the
complete checksum inventory, regular-file and safe-path constraints, manifest
schema and artifact references, and derives a stable `sha256:` identity from
sorted path/size/digest records. Provenance is self-asserted metadata; checksum
consistency is not authenticity.

`floceed fixture admit --input <directory> --policy <policy.yaml>` evaluates
verified facts against a strict, versioned policy. The result binds the policy
digest, fixture identity, evaluation time, and decision. Keep policy files on
the protected repository side of a CI trust boundary. Fixture contents cannot
override the admission policy.

For transport, `floceed fixture pack` creates a deterministic gzip/tar archive
and `floceed fixture unpack` extracts it through an atomic, bounded, no-link
path. Re-run `fixture verify` after unpacking before inspection or replay.

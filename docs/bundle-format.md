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
Potential v0.4 `reused` and `invalidated` capture-ledger outcomes are deferred
and are not part of the v0.3 receipt vocabulary.

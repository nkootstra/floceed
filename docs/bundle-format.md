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

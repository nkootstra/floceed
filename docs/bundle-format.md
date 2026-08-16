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
write manifest schema v2; the renderer and embedded runtime continue to accept
v1. Schema v2 stores DynamoDB data as bounded sorted NDJSON chunks and S3 data
as bounded tar/gzip packs with streamed per-pack indexes, avoiding one manifest
entry and one host file per S3 object. Paths in
the manifest are relative and must remain below `bundle/data/`. The replay
runtime rejects a newer schema, a non-loopback endpoint, an invalid source
account ID, unsafe paths, and changed checksummed files before applying the
stage.

The generated replay hook validates checksums once, then applies base resources,
cross-resource links, and fixture data in that order. Replay creates or updates
selected state but never removes unexpected local resources. Tags are merged.
An incompatible immutable DynamoDB schema or S3 object-lock setting is a hard
failure rather than a delete-and-recreate operation.

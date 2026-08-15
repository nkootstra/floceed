# Generated bundle

`floceed.yaml` is human-owned. `.floceed/` is renderer-owned and replaced only
after a staged bundle passes schema, path, checksum, credential-leak, and
Docker Compose validation.

```text
.floceed/
├── compose.generated.yaml
├── init/ready.d/
│   ├── 10-base-resources.py
│   ├── 30-resource-links.py
│   └── 60-seed-data.py
├── runtime/replay.py
├── bundle/
│   ├── manifest.json
│   └── data/{s3,dynamodb}/
├── checksums.json
└── .gitignore
```

The manifest schema is versioned independently of the project YAML. Paths in
the manifest are relative and must remain below `bundle/data/`. The replay
runtime rejects a newer schema, a non-loopback endpoint, an invalid source
account ID, unsafe paths, and changed checksummed files before applying the
stage.

The three hooks run lexically. Base resources and mutable settings precede
cross-resource links, and fixture data runs last. Replay creates or updates
selected state but never removes unexpected local resources. Tags are merged.
An incompatible immutable DynamoDB schema or S3 object-lock setting is a hard
failure rather than a delete-and-recreate operation.


# Basic project

This example selects one bucket and one DynamoDB table. Replace the profile,
expected account ID, and resource names before running it.

```bash
floceed plan --project floceed.yaml
floceed pull --project floceed.yaml
floceed up --project floceed.yaml
```

`pull` creates `.floceed/compose.generated.yaml`, the `ready.d` hooks,
`runtime/replay.py`, `bundle/manifest.json`, bounded fixture data, and
`checksums.json`. Fixture bytes below `.floceed/bundle/data/` are ignored by
the generated `.floceed/.gitignore`; decide deliberately how they should be
stored and shared.


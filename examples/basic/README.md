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

The example also defines a fictional `share-safe` fixture profile. Inject its
key material at runtime and select it explicitly:

```bash
export FLOCEED_GOVERNANCE_SECRET="$(openssl rand -base64 32)"
floceed plan --project floceed.yaml --fixture-profile share-safe
floceed pull --project floceed.yaml --fixture-profile share-safe --yes
```

`fixtures-2026-08` is a non-secret rotation label. Store the actual secret in a
secret manager and rotate the label whenever the secret changes. Rotation
intentionally changes pseudonyms and cohort membership and invalidates old
checkpoints. This sample is illustrative, not a declaration that the resulting
fixtures are safe: review every rule and uncovered field against your own data
before sharing a bundle. In particular, do not replace pseudonymization with
public `hash` for enumerable identifiers such as email addresses.

Deterministic cohorts require `full` data mode because Floceed must inspect the
complete table. `max_retained_bytes` limits the protected candidate state kept
for resumable selection.

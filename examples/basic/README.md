# Basic project

This example selects one bucket and one DynamoDB table plus fictional
structure-only service entries. Replace the profile, expected account ID, and
resource names before running it.

```bash
floceed init --region eu-west-1
floceed plan --project floceed.yaml
floceed pull --project floceed.yaml
floceed inspect --project floceed.yaml
floceed up --project floceed.yaml
```

`floceed doctor` defaults to `./floceed.yaml`; use `--project` when the file is
elsewhere. See [the configuration reference](../../docs/configuration.md) for
all supported YAML fields and service boundaries.

`pull` creates `.floceed/compose.generated.yaml`, the `ready.d` hooks,
`runtime/replay.py`, `bundle/manifest.json`, bounded fixture data, and
`checksums.json`. Fixture bytes below `.floceed/bundle/data/` are ignored by
the generated `.floceed/.gitignore`; decide deliberately how they should be
stored and shared.

Inspect the installed bundle before starting Docker, emit its stable JSON read
model for automation, or compare it with another project/bundle:

```bash
floceed inspect --project floceed.yaml --output json
floceed inspect --project floceed.yaml --compare ../previous/floceed.yaml
floceed inspect --project floceed.yaml --runtime
```

The first two forms are offline. `--runtime` adds a bounded Floci initialization
readiness check; an unavailable runtime does not invalidate the artifact.
Receipts classify resources as added, removed, changed, or unchanged and report
safe semantic categories without fixture values. A successful pull may also
explain individual capture units as reused, refreshed, or invalidated. These
decisions describe capture work only; they never reconcile or delete local
state.

Repeated pulls keep a runner-local completed-capture ledger under Floceed's
resolved work directory (or the root selected with `--work-dir`). Unchanged S3
units can be reused after a current inventory and local integrity verification;
DynamoDB is conservatively rescanned with reason `freshness_unproven`.
`--restart` clears only the matching interrupted-run checkpoint, not completed
reuse entries. The ledger contains sensitive fixture bytes, can grow without
implicit pruning, and must not be copied between runners. The installed
`.floceed/` directory remains complete and replayable without that ledger.

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

To generate a deterministic representative bundle without AWS credentials,
use the repository fixture generator:

```bash
go run ./internal/testfixture/cmd/generate-bundle \
  -representative \
  -output /tmp/floceed-representative
```

The generated bundle contains one S3 object, one DynamoDB item, one Kinesis
record, and one bounded SQS message, plus SNS structure/topology. Historical
SNS published messages are not available through the AWS API.

Deterministic cohorts require `full` data mode because Floceed must inspect the
complete table. `max_retained_bytes` limits the protected candidate state kept
for resumable selection.

For AWS-free CI checks, verify the generated bundle before inspection or replay:

```bash
floceed fixture verify --input .floceed --output json
```

# floceed

Floceed is a snapshot compiler for [Floci](https://floci.io/). It reads selected
S3 buckets and DynamoDB tables from AWS, normalizes them, and emits a portable
bundle that Floci replays locally through `ready.d` initialization hooks. It is
not a network proxy and never writes to the source account.

CI consumers can verify and admit a generated fixture without AWS credentials:

```bash
floceed fixture verify --input .floceed --output json
floceed fixture admit --input .floceed --policy ci/fixture-policy.yaml --output json
floceed fixture pack --input .floceed --archive fixture.tar.gz
floceed fixture unpack --archive fixture.tar.gz --target .floceed-unpacked
```

Verification proves local integrity and derives a stable fixture identity.
Admission applies repository policy; self-asserted provenance is not treated as
authentic without a protected carrier or external attestation.

## Install

Floceed requires Go 1.26 or newer. Docker and Docker Compose are needed only
for `doctor`, `up`, and integration tests.

```bash
go install github.com/nkootstra/floceed/cmd/floceed@latest
```

For a source checkout:

```bash
go build -o floceed ./cmd/floceed
./floceed --help
```

## AWS authentication

Floceed uses the AWS SDK for Go v2 standard configuration chain. Named
profiles, IAM Identity Center (SSO), credential processes, and ordinary
assume-role profiles work without a floceed credential store.

```bash
aws sso login --profile development
floceed scan --profile development --region eu-west-1 --output json
```

Interactive MFA token prompting for assume-role profiles is not implemented.
Authenticate outside floceed first. Source access must be read-only; see the
[least-privilege policy template](iam-policy.md). Floceed confirms the
caller with STS and can pin `expected_account_id` to prevent capturing from the
wrong account.

## Use

Run `floceed` in a terminal for the keyboard-first TUI. It guides profile and
region selection, confirms caller identity, discovers resources, applies
filters and bounded import options, presents dependencies and compatibility
findings, and asks before saving or capturing.

Headless workflows use the same application services:

```bash
floceed scan --profile development --region eu-west-1
floceed plan --project floceed.yaml
floceed pull --project floceed.yaml --yes
floceed inspect --project floceed.yaml
floceed render --project floceed.yaml
floceed doctor --project floceed.yaml
floceed up --project floceed.yaml
floceed capabilities --output json
```

`capabilities` is offline and reports the supported Floci version, manifest
schemas, services, and data modes for scripts or tooling. Floceed is still
pre-1.0, so compatibility commitments may evolve between minor releases.

To capture shareable fixtures, define a named `fixture_profiles` entry and
select it explicitly for both planning and capture:

```bash
export FLOCEED_GOVERNANCE_SECRET="$(openssl rand -base64 32)"
floceed plan --project floceed.yaml --fixture-profile share-safe
floceed pull --project floceed.yaml --fixture-profile share-safe --yes
```

The secret is read from the environment, must decode to at least 32 random
bytes, and is never stored in the project or bundle. Distribute it through the
same secret manager used by your team, not through Git or shell history. See
[Governed fixture profiles](#governed-fixture-profiles) before sharing output.

Use `--output json` for a stable command envelope and `--no-color` (or
`NO_COLOR=1`) for plain text. Non-terminal stdout never receives TUI control
sequences. Start from [the complete example project](../examples/basic/floceed.yaml).

`scan` inventories AWS. `plan` reads AWS but neither writes generated files nor
starts containers. `pull` captures and atomically installs a new bundle.
`render` reads only the existing local manifest and artifacts. `up` is a thin
wrapper around the generated Compose project and waits for Floci's
`/_floci/init` ready state.

### Inspect and compare bundles

`inspect` is offline by default. It validates the manifest schema, safe paths,
and every indexed checksum before describing the installed bundle. It does not
load AWS credentials, run Docker, contact Floci, or mutate the bundle.

```bash
floceed inspect --project floceed.yaml
floceed inspect --project floceed.yaml --output json
floceed inspect --project floceed.yaml --compare ../previous/floceed.yaml
```

`--compare` also accepts a generated bundle directory. The receipt compares
canonical manifest semantics, not raw files or fixture values. Each resource is
`added`, `removed`, `changed`, or `unchanged`. Changed resources name one or
more stable categories: `structure`, `dataset`, `governance`, `operations`,
`findings`, `selection`, `source`, or `target`. Capture time and producing tool
version do not create semantic changes. Dataset hashes, counts, formats, and
chunk identities can create a semantic change, but a semantic identity never
replaces checksum validation: checksums prove byte integrity; the receipt
explains replay-relevant meaning.

A successful first `pull` reports `baseline: absent`. Later successful pulls
include a receipt comparing the prior valid bundle with the installed bundle.
A failed replacement emits no success receipt and leaves the prior bundle in
place. Floceed does not reconcile, delete, or otherwise correct differences.
The resource outcome remains one of the four listed above. Each selected
resource can also carry capture-unit outcomes: `reused`, `refreshed`, or
`invalidated`, with a stable reason such as `source_content_changed`,
`freshness_unproven`, or `artifact_corrupt`. These subordinate outcomes explain
how the new bundle was captured; they do not add resource classifications or
infer reuse from semantic equality.

Governed schema-3 bundles expose only the audit fields already safe for the
manifest: opaque policy, cohort, rule, and resource identities; non-secret key
IDs; algorithm versions; actions; truncation state; and fixed count buckets.
Inspection and receipts never expose source values, structures, replacement
values, salts, rank digests, audit samples, or fixture records.

Runtime readiness is optional enrichment:

```bash
floceed inspect --project floceed.yaml --runtime
```

When requested, Floceed makes one bounded request to the configured local Floci
endpoint. An unreachable endpoint is reported as `unavailable` while the valid
artifact summary remains available. This is initialization readiness, not a
runtime drift engine, log viewer, or reconciliation mechanism.

### JSON output envelope

`--output json` writes exactly one envelope per invocation:

```json
{"schema_version":1,"command":"scan","status":"success","data":{}}
```

`status` is `success`, `success_with_findings`, or `error`. On failure the
envelope also carries an `error` object with a stable `code`, a human
`message`, and an optional `remediation`, and the process exits with a code
specific to the failure category: usage 2, source 3, partial 4, plan 5,
filesystem 6, local 7, unexpected 1, cancellation 130. Failures still deliver
the successful part of the command's payload in `data`; `doctor` attaches its
check results so consumers can see exactly which prerequisite failed.

### Large datasets and progress

Data capture remains bounded by default. Set `data.mode: full` on an individual
S3 bucket or DynamoDB table to opt into a resumable full export. Full mode
requires an explicit `target.hook_timeout_seconds` greater than 300 because
replaying a large bundle can take hours. S3 prefixes and overwrite policies
still apply; bounded count and byte limits must be omitted in full mode.

```yaml
target:
  hook_timeout_seconds: 21600
  replay_workers: 4
resources:
  s3:
    - name: staging-assets
      data:
        enabled: true
        mode: full
        prefixes: [fixtures/]
        overwrite: if-different
  dynamodb:
    - name: staging-records
      data:
        enabled: true
        mode: full
        gzip: true
capture:
  resource_workers: 2
```

`pull` stores both in-progress checkpoints and a completed-capture reuse ledger
under the resolved work directory. By default that is Floceed's OS user-cache
location (`<user-cache>/floceed/captures`); `--work-dir` selects a different
root. Completed generations and immutable blobs live below
`<resolved-work-dir>/ledger`. An interrupted pull resumes
from its matching checkpoint. A later completed pull separately evaluates the
ledger and may reuse verified S3 capture units. `--restart` discards only the
matching in-progress checkpoint: it does not erase completed ledger entries or
disable their evaluation.

The ledger and its content-addressed blobs are a runner-local optimization, not
part of the bundle and not a portable cache. Do not copy them between machines,
users, or CI runners. They contain captured fixture bytes, use restrictive
permissions, and can grow with successive captures. Floceed v0.4 performs no
implicit pruning; protect and size the work volume as carefully as the generated
bundle. Before reuse, Floceed validates ledger metadata and rechecks every blob's
regular-file status, size, and SHA-256. Missing or corrupt candidates fall back
to a source refresh. A failed refresh, cancellation, or render leaves the prior
installed bundle and prior valid ledger generation intact.

S3 completed-run reuse first performs a current `ListObjectsV2` inventory.
Matching object ETags and sizes are conditional freshness evidence: unchanged
materialization units can be reused without `GetObject`, while added, changed,
missing, or policy-incompatible objects refresh or invalidate their affected
units. ETags are not presented as universal content hashes; captured artifact
SHA-256 checks still protect local byte integrity. DynamoDB has no safe
completed-scan freshness proof in v0.4, so every selected table reports
`freshness_unproven` and is scanned again. This is distinct from resuming an
interrupted scan checkpoint.

Every pull still materializes one complete bundle and installs it atomically.
Reuse does not create layered or delta bundles, replay-time ledger lookups,
tombstones, or destructive source-to-local synchronization. Full capture checks
available disk before bulk transfer and never replaces the previous valid
bundle after a failure or cancellation.

TTY progress shows the current inventory, capture, finalization, installation,
and replay phase. For automation, `--progress json` writes NDJSON progress
events to stderr while `--output json` keeps exactly one final envelope on
stdout. S3 remaining counts become exact after inventory; DynamoDB totals are
clearly marked as estimates until its eventually consistent scan completes.

## Governed fixture profiles

A fixture profile is an explicit, reviewable policy for transforming selected
data before floceed writes a checkpoint, temporary file, or bundle artifact.
The supported actions are `omit`, `replace`, `hash`, and `pseudonymize`.
Supported targets are DynamoDB attribute paths, individual S3 user-metadata
keys, and the whole body of an S3 object whose source content type is on the
rule's allowlist. S3 keys are not rewritten. Structured JSON/CSV fields and
binary object bodies are not transformed in v0.2.0.

`pseudonymize` uses keyed `pseudonym/v1` tokens. The `key_id` is a public,
non-secret rotation label; changing either it or the runtime secret deliberately
breaks linkability and invalidates matching checkpoints. A scope controls where
equality remains linkable, so use the narrowest useful scope. Deterministic
pseudonyms preserve equality and can expose frequency—they are not anonymization.

`hash` uses public, unkeyed `hash/v1`. Do not use it for email addresses,
customer identifiers, phone numbers, status values, or other low-entropy or
enumerable protected data: an attacker can guess inputs and compare hashes. Use
keyed pseudonymization or omission instead.

DynamoDB cohorts select the lowest deterministic `cohort-rank/v1` values from
the configured canonical key attributes. Membership is independent of scan page
and arrival order for an unchanged table, configuration, and secret. The scan is
still eventually consistent, not a point-in-time snapshot. Cohorts require the
resource's data mode to be `full`, because selecting the stable lowest-ranked
set must inspect the complete table. `max_retained_bytes` bounds protected
retained checkpoint state and defaults to 64 MiB. Rank digests are
sensitive because they preserve linkability and remain protected checkpoint
state; they are never emitted in the manifest or command output. Rotation or a
change to policy, cohort keys, predicates, algorithms, or content-type allowlists
invalidates the old checkpoint rather than mixing policies.

Governance audit output is intentionally coarse. It contains opaque rule and
resource identities, non-secret key IDs, algorithm versions, and only these
count buckets: `0`, `1-9`, `10-99`, `100-999`, and `1000+`. It excludes source
values, exact rare counts, target paths, secrets and secret verifiers, rank
digests, and inferred data classifications. Manifest schema 3 carries this
audit metadata; replay continues to accept schema 1 and 2 bundles without it.

Profiles reduce accidental disclosure but cannot determine whether a policy is
safe for your data. The policy author must review uncovered attributes,
metadata, object types, equality/frequency leakage, and the resulting bundle
before it is distributed.

## Safety defaults

- Imports are structure-only unless fixture data is explicitly enabled.
- Bounded S3 fixtures require maximum object count, per-object bytes, and total bytes.
- Bounded DynamoDB fixtures require maximum item and page counts and use eventually
  consistent scans; they are a bounded sample, not a transactionally coherent
  database backup.
- Current S3 object versions only are captured. Content is streamed to hashed
  filenames; raw keys are never filesystem paths.
- Source credentials, session tokens, SSO tokens, and credential-process output
  are neither serialized nor logged. Generated output is scanned for common
  credential patterns before installation.
- Secrets Manager values and secure SSM values are outside the MVP.
- Unexpected partial fixture exports fail closed unless the project explicitly
  permits partial data. Reaching a configured bound is reported as truncation.
- Generated files and fixture data use restrictive permissions. The generated
  `.gitignore` excludes `bundle/data/`; fixture data can still be sensitive, so
  review storage and sharing separately.
- Replay merges tags and upserts data. It does not delete unexpected local
  resources or mutate AWS.
- Governance does not infer PII, generate synthetic data, rewrite S3 keys or
  binary bodies, transform fields inside JSON/CSV, or discover relationship
  closure. Rules apply only to their exact supported targets.

## Generated environment

Floceed writes a separate `.floceed/compose.generated.yaml`; it does not
overwrite a user-maintained Compose file. The bundle includes a JSON manifest,
checksums, an embedded Python/boto3 runtime, and the `10-replay.py` ready hook.
See the [bundle contract](bundle-format.md).

Generated environments pin
`floci/floci:1.6.0-compat@sha256:15ba10dace4a29d94f0e36c03dc3c2ec5bfc4364a1cf9c67f9def4e530ae2c2c`.
Replay signs local requests with the source 12-digit account ID as a dummy
access key, secret `test`, and the source region. These are local-only dummy
credentials that preserve account-scoped local ARNs.

## Floci compatibility

The current capability registry targets Floci 1.6.0 only. S3 lifecycle and
encryption configuration can be stored locally but do not reproduce all AWS
runtime semantics. S3 website redirect/routing behavior is partial. Replication
and access logging are reported but not replayed. Explicitly selected SQS queues
and SNS topics are created as local targets before S3 notification links are
applied. SQS supports bounded message capture. SNS topic structure and
subscriptions remain representational, but historical published SNS messages
are not capturable through the AWS API. Notification events and S3 key filters
are preserved.
Notifications with unselected SQS, SNS, Lambda, or EventBridge targets are
disabled and reported as unresolved dependencies. DynamoDB capacity settings are representational locally, and the
known Floci LSI `INCLUDE` projection readback limitation is surfaced.

Explicitly selected Kinesis streams support bounded or full record capture.
Records are replayed in deterministic shard order; stream retention and
consumer/shard-iterator state are not captured or replayed.

Explicitly selected EventBridge event buses support structure-only capture of
rules, event patterns, and targets. Replay creates buses and upserts their
rules and targets; historical events, archive contents, and replay-time
delivery history are not captured.

Explicitly selected Lambda functions support structure-only capture of runtime
configuration, aliases, and event-source mappings. Function code, layers,
versions' deployment packages, and invocation history are not copied or
executed by replay; use this support to document topology and dependencies
until a safe executable-package contract is available.

Secrets Manager and SSM Parameter Store support is metadata-only. Floceed
records names, ARNs, and non-sensitive metadata, never calls a value-fetching
API, and never writes secret or parameter values to bundles, checkpoints, or
logs. Replay does not create or populate protected values; provide those
through the target environment's own secret management.

Existing tables with incompatible keys or indexes and buckets with incompatible
immutable object-lock state fail without replacement. Floceed does not rewrite
arbitrary strings that look like ARNs.

## Troubleshooting

- For an expired SSO session, run `aws sso login --profile PROFILE` and retry.
- Run `floceed doctor` to check identity, project configuration, Docker,
  Compose, the pinned image, and output permissions.
- Floci health alone does not mean initialization succeeded. Floceed waits for
  `GET /_floci/init` to report `completed.ready: true` and reports failed
  `scripts.ready` entries.
- Increase `target.hook_timeout_seconds` for larger bounded fixture imports.
- A checksum or manifest-schema error means the bundle/runtime pair is not
  trustworthy; run `floceed pull` again instead of editing generated files.
- `floceed up` refuses to start when the generated bundle is missing or
  `compose.generated.yaml` is not a regular file (`BUNDLE_MISSING` /
  `BUNDLE_INVALID`); run `floceed render` (local manifest) or `floceed pull`
  (AWS) to install the bundle.
- If persistent local state conflicts with immutable captured structure, choose
  a fresh persistence volume or resolve the local conflict explicitly.

## Development

```bash
go test ./...
go test -race ./...
go vet ./...
go build ./cmd/floceed
```

Ordinary tests require neither AWS nor Docker. The end-to-end suite is explicit:

```bash
go test -tags=integration ./internal/integration -count=1
```

It starts the exact digest-pinned Floci compat image, mounts the generated
runtime and hooks read-only, verifies an S3 object and DynamoDB item, then
recreates Floci with persistent state and replays the same bundle.

Deterministic metadata-only inspect fixtures can be checked without AWS or
Docker:

```bash
go test ./internal/testfixture -run TestCommittedInspectFixturesMatchGenerator -count=1
go test ./internal/cli -run TestInspectCommittedFixturesOffline -count=1
```

## Non-goals

Floceed does not provide live or bidirectional synchronization, write changes
back to AWS, copy production-scale data by default, copy secret values, support
every Floci service, generate Terraform or CloudFormation, reproduce IAM
semantics, or replace the official Floci CLI.

## License

Apache License 2.0. See [LICENSE](../LICENSE).

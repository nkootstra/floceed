# floceed

Floceed is a snapshot compiler for [Floci](https://floci.io/). It reads selected
S3 buckets and DynamoDB tables from AWS, normalizes them, and emits a portable
bundle that Floci replays locally through `ready.d` initialization hooks. It is
not a network proxy and never writes to the source account.

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
floceed render --project floceed.yaml
floceed doctor --project floceed.yaml
floceed up --project floceed.yaml
```

Use `--output json` for a stable command envelope and `--no-color` (or
`NO_COLOR=1`) for plain text. Non-terminal stdout never receives TUI control
sequences. Start from [the complete example project](../examples/basic/floceed.yaml).

`scan` inventories AWS. `plan` reads AWS but neither writes generated files nor
starts containers. `pull` captures and atomically installs a new bundle.
`render` reads only the existing local manifest and artifacts. `up` is a thin
wrapper around the generated Compose project and waits for Floci's
`/_floci/init` ready state.

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

`pull` checkpoints verified work in the OS user cache and resumes it
automatically. Use `--work-dir` to place working data on another volume or
`--restart` to discard the matching checkpoint. Full capture checks available
disk before bulk transfer and never replaces the previous valid bundle after a
failure or cancellation.

TTY progress shows the current inventory, capture, finalization, installation,
and replay phase. For automation, `--progress json` writes NDJSON progress
events to stderr while `--output json` keeps exactly one final envelope on
stdout. S3 remaining counts become exact after inventory; DynamoDB totals are
clearly marked as estimates until its eventually consistent scan completes.

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
and access logging are reported but not replayed. Notifications with unselected
SQS, SNS, Lambda, or EventBridge targets are disabled and reported as unresolved
dependencies. DynamoDB capacity settings are representational locally, and the
known Floci LSI `INCLUDE` projection readback limitation is surfaced.

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

## Non-goals

Floceed does not provide live or bidirectional synchronization, write changes
back to AWS, copy production-scale data by default, copy secret values, support
every Floci service, generate Terraform or CloudFormation, reproduce IAM
semantics, or replace the official Floci CLI.

## License

Apache License 2.0. See [LICENSE](../LICENSE).

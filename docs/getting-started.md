# Getting started

Floceed compiles selected AWS resources into a portable bundle that Floci can
replay locally. Source access is read-only, structure-only capture is the
default, and fixture data capture must be explicitly enabled and bounded.

## Prerequisites

- Go 1.26 or newer.
- AWS credentials with read-only access to the resources you select.
- Docker and Docker Compose for local replay and integration tests.
- The AWS CLI if you want to inspect replayed resources directly.

Floceed uses the AWS SDK for Go v2 standard credential chain. Named profiles,
IAM Identity Center sessions, credential processes, and assume-role profiles
are supported. Authenticate outside Floceed first, for example:

```bash
aws sso login --profile development
```

Start from the [least-privilege IAM policy](iam-policy.md) and consider setting
`expected_account_id` so a project cannot capture from the wrong account.

## Install

Install the current command from Go:

```bash
go install github.com/nkootstra/floceed/cmd/floceed@latest
floceed version
```

For a source checkout, build the command instead:

```bash
go build -o floceed ./cmd/floceed
./floceed version
```

## Capture and replay a project

Create a project for the AWS region you want to inspect:

```bash
floceed init --profile development --region eu-west-1
```

Scan the account to discover supported resources:

```bash
floceed scan --profile development --region eu-west-1
```

Edit `floceed.yaml` to select resources. Begin with structure-only capture and
review the planned operations before capturing anything:

```bash
floceed plan --project floceed.yaml
floceed pull --project floceed.yaml --yes
```

Verify the generated bundle offline, then start Floci:

```bash
floceed fixture verify --input .floceed
floceed up --project floceed.yaml
```

When Floci is ready, inspect the local resources through its AWS-compatible
endpoint:

```bash
aws --endpoint-url http://localhost:4566 s3 ls
aws --endpoint-url http://localhost:4566 dynamodb list-tables
```

Use `floceed doctor --project floceed.yaml` when a prerequisite or local
runtime check fails. Use `floceed inspect --project floceed.yaml` to validate a
bundle without contacting AWS or Docker.

## Governed fixture capture

Do not treat pseudonymization as proof that a fixture is anonymous or safe to
share. Review the selected fields, limits, governance profile, and generated
bundle before distributing it.

For the example project's fictional governance profile:

```bash
export FLOCEED_GOVERNANCE_SECRET="$(openssl rand -base64 32)"
floceed plan --project examples/basic/floceed.yaml --fixture-profile share-safe
floceed pull --project examples/basic/floceed.yaml --fixture-profile share-safe --yes
```

Keep the governance secret in your team's secret manager. Never put it in Git,
the project file, or shell history. Read the [governed fixture profile
guidance](README.md#governed-fixture-profiles) before capturing shareable data.

## Try verification without AWS

The repository includes a deterministic representative-bundle generator. It
uses fictional values and does not contact AWS:

```bash
go run ./internal/testfixture/cmd/generate-bundle \
  -representative \
  -output /tmp/floceed-representative

floceed fixture verify \
  --input /tmp/floceed-representative/.floceed \
  --output json
```

The [basic example](../examples/basic/) contains a complete project file and
additional guidance for inspection, fixture governance, and CI admission.

## Next references

- [Configuration reference](configuration.md)
- [Bundle format](bundle-format.md)
- [Least-privilege IAM policy](iam-policy.md)
- [Detailed documentation](README.md)

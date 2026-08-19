# Floceed

**Floci gives you local AWS. Floceed gives it reproducible state.**

Floceed (**Floci + seed**) is a Go TUI and CLI that reads selected AWS
resources, applies bounded capture and governance policies, and compiles a
portable bundle that [Floci](https://floci.io/) can replay locally.

It is a snapshot compiler, not a network proxy or synchronization service.
Floceed only reads from the source AWS account and never writes to it.

New here? Start with the [getting started guide](docs/getting-started.md).
Contributors can read [CONTRIBUTING.md](CONTRIBUTING.md), and security reports
belong in the process described by [SECURITY.md](SECURITY.md).

## Why Floceed?

- Reproduce data-dependent bugs locally without repeatedly using AWS.
- Give developers and CI a realistic, reviewable starting state.
- Capture structure by default and opt into bounded fixture data explicitly.
- Verify and admit bundles offline, without AWS credentials.
- Keep replay artifacts portable and compatible with Floci initialization hooks.

> **Governed does not automatically mean anonymous.** Floceed provides explicit
> controls and safe defaults, but fixture authors remain responsible for
> reviewing a bundle before sharing it. Pseudonymization reduces exposure; it
> is not a guarantee that a fixture is anonymous or safe to publish.

## Quick start

Requirements: Go 1.26+ and AWS credentials with read-only access. Docker and
Docker Compose are required for local replay.

```bash
go install github.com/nkootstra/floceed/cmd/floceed@latest

floceed init --profile development --region eu-west-1
floceed scan --profile development --region eu-west-1
floceed plan --project floceed.yaml
floceed pull --project floceed.yaml --yes
floceed fixture verify --input .floceed
floceed up --project floceed.yaml
```

For a governed fixture profile, select it explicitly during planning and
capture:

```bash
export FLOCEED_GOVERNANCE_SECRET="$(openssl rand -base64 32)"
floceed plan --project floceed.yaml --fixture-profile share-safe
floceed pull --project floceed.yaml --fixture-profile share-safe --yes
```

See the [basic example](examples/basic/), or generate a deterministic,
AWS-free representative bundle:

```bash
go run ./internal/testfixture/cmd/generate-bundle \
  -representative -output /tmp/floceed-representative
```

## Safety model

- Source access is read-only; Floceed confirms the caller with STS.
- Structure-only capture is the default.
- Data capture is bounded unless full mode is explicitly configured.
- Governance policies support omission, replacement, hashing, and
  pseudonymization.
- Secret values are not captured as fixture data.
- Bundles contain checksums and can be verified offline.
- Fixture admission can be enforced by repository policy.

Review the [least-privilege IAM policy](docs/iam-policy.md) and the
[governed fixture profile guidance](docs/README.md#governed-fixture-profiles)
before capturing or sharing data.

## Supported services

Data capture is available for S3, DynamoDB, and Kinesis in bounded or full
mode; SQS supports bounded capture. SNS, EventBridge, Lambda, Secrets Manager,
SSM, API Gateway, Step Functions, and CloudWatch Logs are currently supported
for structure or metadata, subject to the capability report.

```bash
floceed capabilities
```

## Compatibility

The current target is **Floci 1.6.0** with manifest schemas **1–3**. Check the
capability report for the exact runtime contract:

```bash
floceed capabilities --output json
```

Floceed is pre-1.0, so compatibility commitments may evolve between minor
releases.

## Documentation

- [Getting started](docs/getting-started.md)
- [Configuration reference](docs/configuration.md)
- [Bundle format](docs/bundle-format.md)
- [IAM policy template](docs/iam-policy.md)
- [Basic example](examples/basic/)
- [Detailed documentation](docs/README.md)

## Development

```bash
go test ./...
go vet ./...
go build ./cmd/floceed
```

Integration tests require Docker and are run with the `integration` build tag.

## License

See [LICENSE](LICENSE).

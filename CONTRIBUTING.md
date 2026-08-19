# Contributing to Floceed

Thank you for helping improve Floceed. Contributions should make local AWS
state more reproducible without weakening the source-account or fixture-safety
boundaries.

## Before you start

- Read the [getting started guide](docs/getting-started.md) and the relevant
  reference documentation.
- For larger changes, open an issue or discussion first so the compatibility
  and Floci impact can be understood.
- Do not include AWS credentials, secret values, production data, or
  production-derived fixture values in issues, pull requests, tests, or
  documentation.

## Local development

Floceed requires Go 1.26 or newer. Docker and Docker Compose are required for
the integration suite and local Floci replay.

Run the normal checks before opening a pull request:

```bash
go test ./...
go test -race ./...
go vet ./...
go build ./cmd/floceed
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest runtime/test_replay.py
PYTHONDONTWRITEBYTECODE=1 python3 -m py_compile runtime/replay.py
```

Run the integration suite separately when Docker is available:

```bash
go test -tags=integration ./internal/integration -count=1 -v
```

The authoritative checks are defined in `.github/workflows/ci.yml`.

## Fixtures and test data

Use the deterministic generators under `internal/testfixture/` for committed
fixtures and examples. Do not hand-edit generated fixture output when a
generator exists. Run the fixture consistency and offline inspection checks
when changing committed fixtures.

Fixture governance is explicit but does not automatically make data anonymous.
Review every covered and uncovered field before sharing a bundle.

## Pull requests

Keep pull requests focused and explain:

- What behavior or documentation changed and why.
- Which Floci version and manifest schemas are affected.
- Which checks you ran, including any checks unavailable in your environment.
- Whether the change affects capture limits, governance, source permissions, or
  fixture contents.

Update documentation and tests with behavior changes. Do not add promotional
or generated attribution blocks to commits, pull requests, or changelogs.

## Reporting problems

Use the issue templates for ordinary bugs and feature requests. Do not report a
security vulnerability in a public issue; follow [SECURITY.md](SECURITY.md).

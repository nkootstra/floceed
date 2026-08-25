# Floceed documentation

Floceed compiles selected AWS resources into portable bundles for Floci. The
root [README](../README.md) is the project overview; this page is only the
documentation index.

## Guides

- [Getting started](getting-started.md) — install, configure, capture, and replay.
- [Configuration](configuration.md) — project schema, resource selection, and data bounds.
- [IAM policy](iam-policy.md) — source-account permissions and preflight behavior.
- [Bundle format](bundle-format.md) — manifest, artifact, checksum, and provenance contracts.
- [Compatibility](compatibility.md) — pinned Floci behavior and governance boundaries.

## Development

- [Plans](plans/) — implementation plans and historical design records.
- [Ideation](ideation/) — exploratory notes; these are not normative contracts.

The focused documents above own their respective contracts. When behavior
changes, update the focused document and the relevant tests rather than adding
a second copy of the rule here.

# Compatibility and governance boundaries

Floceed targets the pinned Floci 1.6.0 image and preserves the manifest schema
contract used by that runtime. Replay bundles are portable artifacts; capture
does not proxy network traffic or modify the source AWS account.

Capture defaults to structure-only. Data capture is bounded unless a resource
explicitly opts into full mode, and full-mode captures use the extended replay
timeout. Verification and admission are offline operations and must not require
AWS credentials.

The source account remains read-only. The permissions required for discovery,
structure capture, and bounded data capture are documented in
[iam-policy.md](iam-policy.md). Fixture governance is an explicit admission
boundary: local secret material is used for policy evaluation and is never
written into manifests, AWS metadata, or IAM policies.

## Governed fixture profiles

Governance profiles are local policy input for fixture admission and data
transformation. Keep the governance secret outside the project file and source
control. Rules may omit, replace, hash, or pseudonymize selected fields; the
runtime never treats a governance secret as captured fixture data.

See [bundle-format.md](bundle-format.md) for the wire-level artifact contract
and [configuration.md](configuration.md) for schema-owned defaults and bounds.

## Safety defaults

Source access is read-only. Structure-only capture is the default, data reads
are bounded unless full mode is explicit, and verification/admission operate
offline. Capture is bounded by configured item, byte, page, and worker limits;
there is no unbounded source dump mode.

## Generated environment

The replay environment is generated from the committed Floci-compatible
compose template. Do not hand-edit generated compose or manifest output;
change the source project or the generator and regenerate the bundle.

## Floci compatibility

The target image remains pinned to Floci 1.6.0. Upgrade it only with an
explicit compatibility review covering the manifest schema, replay hooks,
service topology, and integration fixtures.

## Troubleshooting

Run `floceed doctor` before capture to validate local prerequisites and source
identity. Permission failures should be fixed on the read-only source policy;
do not work around them by adding credentials to fixtures. For bundle failures,
run offline verification first and inspect the reported path or checksum.

## CLI output and progress

`--output json` emits exactly one envelope per invocation with
`schema_version`, `command`, `status`, and either `data` or `error`. Progress
events are separate NDJSON on stderr when `--progress json` is requested, so
automation can consume the final stdout envelope without interleaved logs.

## Large datasets and reuse

Full S3, DynamoDB, and Kinesis captures are resumable and require an explicit
hook timeout above the default. Full S3 mode is explicit and can capture all
selected data; it bypasses configured object and byte limits. SQS remains
bounded-only. In-progress
checkpoints and completed capture-reuse ledgers live under the resolved work
directory and are runner-local optimizations, not portable bundle contents.
Completed ledger blobs are immutable and revalidated before reuse; they are
not implicitly pruned.

## Non-goals

Floceed is not a network proxy, an AWS mutator, a production data migration
tool, or a substitute for IAM governance. It compiles selected snapshots for
offline replay and keeps source-account writes outside its contract.

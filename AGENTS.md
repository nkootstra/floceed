# Floceed agent instructions

- Treat `cmd/floceed` as the CLI entry point and `runtime/` as the replay runtime.
- Preserve the pinned Floci 1.6.0 compatibility target and manifest-schema contract unless the task explicitly changes them.
- Read the relevant documentation and existing tests before changing behavior.
- For bug fixes, write a failing regression test first; use red-green-refactor and keep the regression test permanently.
- Test behavior through public interfaces. Mock system boundaries only; do not mock internal collaborators.
- Never use real AWS credentials, secrets, production data, or production-derived fixture values in tests, examples, issues, or documentation.
- Use the deterministic generators under `internal/testfixture/` for committed fixtures; do not hand-edit generated fixture output.
- Preserve Floceed’s source-account read-only behavior, structure-only defaults, bounded capture, offline verification, and explicit governance boundaries.
- Run the checks defined in `.github/workflows/ci.yml` before finishing. Integration tests require Docker and use the `integration` build tag.
- Run the replay-runtime tests when changing `runtime/` or generated replay behavior.
- Keep user-facing behavior and compatibility changes documented in the relevant files under `docs/` and `README.md`.
- Report security vulnerabilities through the private process in `SECURITY.md`, never through public issues or pull requests.

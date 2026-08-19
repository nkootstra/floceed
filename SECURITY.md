# Security policy

Floceed reads selected AWS resources and generates local replay fixtures. The
source account is read-only: Floceed does not write to AWS. Structure-only
capture is the default, data capture is explicitly enabled and bounded, and
secret values are not captured as fixture data.

These safeguards do not make every generated fixture anonymous or safe to
share. Fixture authors remain responsible for reviewing captured content and
governance coverage before distribution.

## Supported versions

Floceed is pre-1.0. Security fixes are made against the current development
line and the most recent published version, where practical. Upgrade to the
latest version before requesting help with a vulnerability.

## Reporting a vulnerability

Use the **Report a vulnerability** button on this repository's [Security
advisories](https://github.com/nkootstra/floceed/security/advisories) page.
GitHub private vulnerability reporting is enabled for this repository. Do not
open a public issue, discussion, or pull request containing security details.

Include enough information to reproduce the issue safely, such as the affected
version or commit, command and configuration context, impact, and a minimal
reproduction that does not contain credentials or real AWS data.

The maintainers will acknowledge the report privately, investigate it, and
coordinate disclosure and remediation with the reporter. Please avoid public
disclosure until a fix or coordinated disclosure plan is available.

If private vulnerability reporting is unavailable, do not publish sensitive
details. Contact the repository maintainers through an existing private GitHub
channel and ask for the current reporting path.

## Data-handling reminders

- Never commit AWS access keys, session tokens, governance secrets, or secret
  values.
- Treat captured fixture bytes and local capture-ledger contents as sensitive.
- Review pseudonymization, omission, replacement, and hashing rules before
  sharing a bundle.
- Pseudonymization reduces exposure but is not equivalent to anonymization.

# Contributing

Start with the [development conventions]. They are the canonical policy for
code, tests, documentation, and human responsibility for submitted changes.

[development conventions]: docs/conventions.md

## Getting started

1. Fork the repository and clone your fork.
2. Install the tools listed in the [development guide].
3. Enable commit signing and install the hook with `make setup-hooks`.
4. For code changes, run `make all` and component-specific checks before
   submitting.

The [verification matrix] explains what each local gate covers and which
additional jobs CI runs. `make test` covers the root workspace; Gateway has a
separate workspace.

For README or Markdown prose-only changes, check spelling, Markdown style,
local links and anchors, and whitespace (`git diff --check`). The full test
suite is not required. Examples and executable instructions need validation
appropriate to the affected behavior. Record checks that could not be run and
their prerequisites in the PR.

[development guide]: docs/development.md
[verification matrix]: docs/development.md#verification

## Picking up an issue

Choose a maintainer-triaged issue with a milestone and project assignment at
`priority/medium` or `priority/low`. Maintainers assign urgent and high-priority
work. Coordinate ownership on the issue before beginning a substantial change.
The checked-in issue workflow updates triage labels when milestones change;
it does not enforce assignment or project-board policy.

## Larger changes

Features spanning multiple PRs, new architectural patterns, and public-interface
changes go through the [proposal process]. Keep each implementation PR focused
on a reviewable result.

[proposal process]: https://github.com/praxis-proxy/enhancements/blob/main/docs/process.md

## Pull requests

Explain the problem, resulting behavior, and validation in the PR description.
Follow the [PR conventions], including conventional commit subjects, human
attribution, cryptographic signing, and a `Signed-off-by` trailer:

```console
git commit -S -s -m 'docs: clarify the installation prerequisites'
```

The checked-in Coding Conventions workflow checks sign-off on non-draft PRs
targeting `main`, unless the `skip/signoff` label skips that check. GitHub's
`main` ruleset adds signature, test, and review requirements. A skipped check
does not establish compliance. Other reviewability requirements are contributor
and reviewer obligations; see the canonical policy for the enforcement boundary.
Draft status does not remove the requirement to review and understand the
submitted code.

[PR conventions]: docs/conventions.md#pull-request-conventions

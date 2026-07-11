# Contributing to OCache

Thanks for your interest in contributing! OCache is licensed under the
[Apache License 2.0](LICENSE), and contributions are accepted under the same
license.

## Developer Certificate of Origin (DCO)

We use the [Developer Certificate of Origin](DCO) (DCO) rather than a Contributor
License Agreement. The DCO is a lightweight way for you to certify that you wrote,
or otherwise have the right to submit, the code you are contributing.

You certify to the DCO by adding a `Signed-off-by` line to each commit message:

```
Signed-off-by: Jane Developer <jane@example.com>
```

The name and email **must match** the commit author. Git can add this line for
you automatically:

```bash
git commit -s -m "your message"
```

By signing off, you agree to the terms in the [DCO](DCO) file.

### Fixing a missing sign-off

If CI reports a commit without a sign-off:

```bash
# amend the most recent commit
git commit --amend -s --no-edit

# or sign off a range of commits (e.g. the last 3), then force-push your branch
git rebase --signoff HEAD~3
git push --force-with-lease
```

A CI check (`.github/workflows/dco.yml`) verifies every non-merge commit in a
pull request is signed off.

## Development

- Build and test via the `make` targets (they set the required CGO/RocksDB
  flags) — e.g. `make build`, `make test`, `make lint`. See the
  [README](README.md) and [docs/](docs/) for details.
- Please run `make lint` and `make license-check` before opening a PR.
- Keep PRs focused; write a clear description of the change and its motivation.

## Reporting issues

Open a GitHub issue with steps to reproduce, expected vs. actual behavior, and
relevant version/config details.

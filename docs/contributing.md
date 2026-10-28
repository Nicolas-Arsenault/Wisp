# Contributing

## Issues

New issues use a template, from `.github/ISSUE_TEMPLATE/`; blank issues are only available to maintainers.

| Template | Label | Use it for |
|---|---|---|
| Bug report | `bug` | Something doesn't work as documented. |
| Feature request | `enhancement` | New behavior or capability. |
| Refactor | `refactor` | Improving code without changing what it does. |

The `refactor` label must exist in the repo for the Refactor template to apply it: `gh label create refactor --color c5def5 --description "Code improvement without behavior change"`.

## Pull requests

`.github/pull_request_template.md` fills in every new PR. It asks for:

- **Summary:** what the PR does, and the issue it closes.
- **Type of change.**
- **Breaking changes:** API, proto, on-disk format or CLI flags.
- **Testing:** how it was tested.
- **Checklist:** the same checks CI runs (see [ci.md](ci.md)), plus docs and proto regeneration.

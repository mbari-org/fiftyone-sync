# AGENTS.md

## Project overview

**fiftyone-sync** — Service for syncing data to/from Tator postgres database to Voxel51 mongodb. semantic-release on `main`.

**Architecture map:** `graphify-out/GRAPH_REPORT.md`; refresh with `graphify update .` after code changes.

---

## File headers (required)

Add this 3-line header to **every new or substantially edited** Python module under `src/app/` and `tests/`:

```python
# iftyone-sync, Apache-2.0 license
# Filename: <path relative to root>
# Description: <one-line summary of the module>
```

Example (`src/app/sync_lock.py`):

```python
# fiftyone-sync, Apache-2.0 license
# Filename: src/app/sync_lock.py
# Description: Mutex for FiftyOne sync so only one sync per version runs at a time (Redis-based).
```

---

## Commits (semantic-release)

Use **Angular-style** commit messages so `python-semantic-release` can version correctly (`pyproject.toml`).

**Format:** `<type>[optional scope]: <description>`

**Allowed types:** `feat`, `fix`, `perf`, `docs`, `build`, `ci`, `chore`, `style`, `refactor`, `test`

| Type | Release impact |
|------|----------------|
| `feat` | Minor bump |
| `fix`, `perf` | Patch bump |
| Others | Typically no version bump (see changelog exclude patterns) |
 
Do **not** use ad-hoc prefixes (`Update`, `WIP`, version-only messages) for changes that should ship.

Always update the README.md or docs when adding new features or changing existing behavior.

---

## Pull requests

- **Max size:** 400 lines changed (additions + deletions) per PR. Split larger work into stacked or sequential PRs.
- **Labels (required):** Apply GitHub labels in this form before requesting review:

| Label | Meaning (pick one per dimension) |
|-------|----------------------------------|
| `type/feature` | New capability |
| `type/fix` | Bug fix |
| `type/docs` | Documentation only |
| `type/refactor` | Behavior-preserving restructure |
| `type/test` | Tests only |
| `type/chore` | Tooling, deps, CI |
| `scope/app` | `src/app/` |
| `scope/tests` | `tests/` |
| `scope/infra` | CI, Docker, packaging |
| `impact/low` | Small, localized risk |
| `impact/medium` | Moderate behavior or API surface |
| `impact/high` | Breaking or wide blast radius |
| `status/needs-review` | Ready for human review |

Example set: `type/feature`, `scope/generators`, `impact/medium`, `status/needs-review`

---

## Testing

Run tests from the repo root after substantive changes (see `README.md` / project docs for full setup).

---

## Graphify

Before answering architecture or “how does X connect to Y?” questions, read `graphify-out/GRAPH_REPORT.md`. After editing code in a session, run `graphify update .` (AST-only, no API cost).

---

## Tools

- **gh (GitHub CLI):** `/opt/homebrew/bin/gh`

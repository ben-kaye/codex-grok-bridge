# AGENTS.md

This project presents Grok Build as a first-class agent in the Codex desktop UI.

## Design discipline

Before changing behavior, read the applicable files in this order:

1. `design/invariants/`
2. `design/feats/`
3. `design/plans/`

Keep design docs concise. Prefer invariants, rubrics, and acceptance criteria to
dense prose.

- Invariants are stable system constraints.
- Feature docs define user-visible outcomes and acceptance criteria.
- Plans define implementation order and verification, and may be replaced.

Update the applicable docs in the same change when behavior or boundaries move.

## Implementation rules

- Understand the end-to-end protocol flow before editing.
- Default to the least code: existing helpers, standard library, installed
  dependencies, then new code.
- No speculative abstractions, compatibility layers, or scaffolding.
- Keep protocol translation separate from transport and process lifecycle.
- Preserve validation, security boundaries, cancellation, and error handling.
- Non-trivial protocol mapping leaves one focused runnable test.
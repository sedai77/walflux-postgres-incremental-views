## What & why

<!-- Small, focused diffs with a clear "why" land quickly. -->

## Checklist

- [ ] `uv run --extra dev pytest -m "not integration"` passes
- [ ] `uv run --extra dev ruff check .` is clean
- [ ] Interface changes update [`docs/SPEC.md`](../docs/SPEC.md) — interface changes are spec changes
- [ ] Behavior changes have tests; anything touching the apply/checkpoint/ack path keeps the `kill -9` integration test honest

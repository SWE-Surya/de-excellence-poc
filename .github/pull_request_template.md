## What does this PR do?

<!-- One or two sentences. What changed and why. -->

## Type

- [ ] Feature (new pipeline logic)
- [ ] Fix (bug)
- [ ] Refactor
- [ ] Config / infra
- [ ] Docs

## How to test

<!-- How a reviewer can verify this change. e.g. "Run `pytest tests/test_silver.py`" -->

## Checklist

- [ ] Code is in version control only; nothing lives solely in the workspace (COD-01)
- [ ] Branched off `main`; no direct commits to `main` (COD-02)
- [ ] Linter/formatter passes (ruff / sqlfluff) (COD-04)
- [ ] Transformation logic has unit tests covering edge cases, and they pass (COD-05)
- [ ] End-to-end / contract check considered or updated if the pipeline changed (COD-06)
- [ ] Promotion/deploy runs through the bundle/CI, not manual copying (COD-07)
- [ ] No secrets, credentials, or hardcoded tokens anywhere in the diff or history (COD-08)
- [ ] Tests and CI checks pass
- [ ] Docs / evidence map updated if behaviour changed

## Notes for the reviewer

<!-- Anything specific you want the reviewer to look at. -->
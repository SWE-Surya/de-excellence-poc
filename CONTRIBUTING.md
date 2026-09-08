# Data Engineering Excellence POC — Evidence Map

**Topic:** Code, Version Control & Testing
**Repo:** `de-excellence-poc`
**Workspace:** `lending_analytics` (AWS)
**Catalog / schemas:** `de_excellence_poc.surya_dev` / `surya_test` / `surya_prod`
**Source system:** `marketplace_india.silver` (read-only, cross-catalog)
**Owner:** suryamani.sudhakar@cloudkaptan.com

> Living tracker. Update the **Status** and **Evidence** columns as each control is
> built. Becomes the backbone of the final best-practice document.
> Status legend: ⬜ Not started · 🟡 In progress · ✅ Done

---

## Use case

A medallion (bronze → silver → gold) pipeline over lending-application data.
The pipeline reads curated tables from an upstream domain (`marketplace_india.silver`)
as its source system, lands a faithful copy in **bronze**, applies real
transformation logic in **silver** (income aggregation, null handling, dedup, join),
and produces a business aggregate in **gold**. The pipeline itself is deliberately
simple; the POC's purpose is to demonstrate the eight engineering-practice controls
below, each with clickable evidence.

**Tables in scope:** `fact_loan_application` (fact) + `dim_income` (dimension),
joined on `application_id`. (`dim_expenses`, `dim_bank_account` are stretch scope.)

---

## Controls

| Ref | Control | Status | Evidence (file / link / screenshot) | Reference |
|---|---|---|---|---|
| COD-01 | All code in version control; nothing untracked in workspace | 🟡 | Repo `de-excellence-poc`; pipeline deployed only via bundle, no workspace-only notebooks | Databricks: Bundles overview |
| COD-02 | Branching & merge strategy + branch protection configured | ⬜ | *(pending: CONTRIBUTING.md + GitHub branch protection)* | GitHub Flow / trunk-based dev |
| COD-03 | Peer review mandatory before merge, with evidence | ⬜ | *(pending: PRs with reviewer + comments)* | Standard PR review |
| COD-04 | Written coding standard + linter enforced in CI | ⬜ | *(pending: standard doc; ruff in CI)* | Ruff docs; Databricks best-practices |
| COD-05 | Unit tests for transformation logic (edge cases, DataFrame equality, in CI) | 🟡 | `tests/test_bronze.py` (2 passing, incl. `assertDataFrameEqual`); local Spark session in `tests/conftest.py` | Apache Spark PySpark testing guide |
| COD-06 | Integration/E2E test + pre-deploy schema contract check | ⬜ | *(pending: E2E test; contract check vs `information_schema`)* | Data Engineering Standards § COD-06 |
| COD-07 | Automated promotion via CI (`bundle deploy`), no manual copy | 🟡 | `databricks.yml` with dev/test/prod targets *(pending: CI workflow running deploy)* | Databricks: Asset Bundles (`bundle deploy`) |
| COD-08 | Secrets externalised; none in code or history | 🟡 | Secret scope `de-excellence-poc` created *(pending: gitleaks in CI; scope/key usage or documented no-secret-by-design)* | Databricks: secret scopes (`dbutils.secrets`) |

---

## Design decisions (with rationale — for the final doc)

| Decision | Choice | Why / trade-off |
|---|---|---|
| Language | PySpark-forward | Matches team stack; COD-05 tests cleanest in PySpark; SQL version planned as phase 2 |
| Logic vs I/O | Pure transformation functions, thin I/O wrappers | Enables unit tests on fixtures, not live catalog (COD-05 pt 3) |
| Environments | 3 targets (dev/test/prod) in one workspace, isolated by schema | Deviation from separate-workspace ideal — named deliberately |
| Source access | Read `marketplace_india.silver` in place (no clone) | Real source→pipeline boundary; avoids data duplication |
| Bronze write mode | `overwrite` | Idempotent for POC; prod would append/merge — noted simplification |
| Test Spark session | Local `master("local[1]")`, not Databricks Connect | Unit tests must be local/deterministic (COD-05); Connect reserved for E2E |

---

## Known deviations / limitations (declare, don't hide)

- **Single workspace** instead of separate dev/test/prod workspaces — isolated by
  schema + bundle target. Deliberate POC-scope decision.
- **Solo peer review** — as a single engineer, COD-03 is demonstrated with a
  genuine reviewer where possible; the process + branch protection are configured
  and the limitation is stated openly.
- **Free-tier GitHub** — some branch-protection features may be unavailable; flagged
  where hit rather than worked around silently.
- **COD-08** — the pipeline uses Unity Catalog-governed access (no embedded
  credential by design); the secret-scope pattern is demonstrated on a config value
  and/or the no-secret-by-design rationale is documented.

---

## Progress log (append per milestone)

- **Setup** — Catalog, 3 schemas, secret scope, CLI auth (OAuth profile
  `lending_analytics`) created and verified.
- **Bundle** — `databricks bundle init` (default-python, serverless); adapted to
  3 targets + `surya_*` schemas + pinned profile; `bundle validate` passes on all targets.
- **Bronze** — PySpark ingestion (faithful copy + metadata) with I/O-vs-logic
  separation; 2 unit tests passing locally against fixtures (COD-05 evidence begins).




  # Contributing

This document defines the branching model and review process for this repository.
It exists to satisfy — and to make routine — the engineering controls under
**Code, Version Control & Testing** (COD-01 to COD-08).

## Branching model

We use a lightweight trunk-based / GitHub-flow model:

- **`main` is always deployable.** It is protected: no direct pushes, no
  force-pushes, no deletion. All changes reach `main` through a pull request.
- **Feature branches** are short-lived and branch off `main`. Name them by
  purpose with a type prefix:
  - `feat/…` — new pipeline logic (e.g. `feat/silver-income-aggregation`)
  - `fix/…` — bug fixes
  - `docs/…` — documentation only
  - `chore/…` — tooling, config, housekeeping
- Keep branches small and focused — one logical change per branch, so the PR is
  easy to review.

## Pull request process

1. Create a feature branch off the latest `main`.
2. Make your change, with tests where logic is involved.
3. Push the branch and open a pull request against `main`.
4. The PR must pass all required status checks (linting and tests in CI).
5. The PR is reviewed by another engineer (see *Review* below) before merge.
6. Merge to `main` (squash merge preferred to keep history readable).
7. Delete the feature branch after merge.

No change is committed directly to `main`, regardless of size or urgency.

## Review

- Every PR is reviewed before merge. Reviewers check not just "does it run", but:
  - correctness and edge-case handling of transformation logic,
  - presence and quality of tests,
  - adherence to the coding standard,
  - that no secrets, dead code, or commented-out blocks are introduced.
- Review comments are left on the PR as visible evidence.
- A PR is not approved by its own author.

> **POC note:** during this proof-of-concept the repository has a single active
> engineer. Where a second reviewer is unavailable, the review process and branch
> protection are configured and enforced, and the single-reviewer constraint is
> documented as a known limitation to be met in a team setting. This is stated
> openly rather than worked around.

## Coding standard

Code follows the standard in `docs/CODING_STANDARD.md`, enforced mechanically by
linters/formatters in CI (not left to manual discretion).

## Secrets

No credential ever appears in code, notebooks, config, or git history. Secrets are
read at runtime from a managed secret store by scope/key. Any secret accidentally
committed is rotated, not merely removed from a later commit.
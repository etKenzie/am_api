# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A FastAPI backend ("Aku Maju API") that serves an executive dashboard for Valdo Group companies. It exposes read-mostly analytics endpoints over an existing MySQL database (`td_karyawan`, `td_loan`, `td_loan_history`, `tbl_gmc`, `payroll_header`/`payroll_detail`, etc.) plus a separate set of AI endpoints (resume scoring, interview scoring, transcription, HeyGen avatar video). There is no ORM data model layer — almost all data access is raw SQL via `sqlalchemy.text()`.

**Note:** `main.py` at the repo root is an unrelated standalone Amazon Transcribe example script, not part of the real app. The actual app entrypoint is `src/main.py`.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run locally with auto-reload (loads src/ onto path, serves on :8001)
python run_local.py

# Run the way Docker/production does (serves on :8000)
python -m uvicorn src.main:app --host 0.0.0.0 --port 8000

# Run via Docker Compose (loads .env, mounts ./src, port 8000)
docker-compose up --build

# Deploy: build/push image to AWS ECR (requires AWS CLI configured)
./deploy.sh
```

There is no test suite, linter, or formatter configured in this repo (no pytest, no `tests/` dir, no `.flake8`/`ruff`/`black` config). Verify changes by hitting the running API (e.g. `curl localhost:8000/health`, or the interactive docs at `/docs`).

Config is via `.env` at repo root (loaded by both `src/db.py` and `src/ai/router.py` via `python-dotenv`): `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`, `OPENAI_API_KEY`, `AWS_REGION`, `S3_BUCKET`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `HEYGEN_API_KEY`. `src/db.py` has hardcoded fallback defaults for the DB_* vars if `.env` is missing — don't rely on those in anything other than local scratch use.

## Architecture

### Router composition
`src/main.py` creates the FastAPI app, wires CORS, and includes `src/router.py`, which in turn aggregates four domain routers plus one plain endpoint module:

- `src/endpoint.py` — `/` and `/health` (no prefix)
- `src/loan/router.py` — prefix `/loan` — loan/kasbon/coverage analytics (by far the largest domain)
- `src/ai/router.py` — prefix `/ai` — resume/interview scoring, transcription, avatar video
- `src/external_payroll/router.py` — prefix `/external_payroll` — payroll analytics for client (non-Valdo) companies
- `src/internal_payroll/router.py` — prefix `/internal_payroll` — payroll analytics for Valdo's own employees (filtered to `valdo_inc` in `{VI, VSDM}`)

Each domain package follows the same three-file shape: `router.py` (FastAPI routes, request/response wiring, try/except that degrades to a `{"status": "error", ...}` payload with zeroed fields rather than raising), `schemas.py` (Pydantic response models — note there's no request-body validation on the GET-heavy loan/payroll endpoints, just query params), `crud.py` (raw SQL query builders, no ORM models).

Every module-internal import in this codebase is wrapped in try/except ImportError, trying relative imports first (`from .foo import x`, used under Docker/`src.main:app`) and falling back to absolute (`from foo import x`, used when `run_local.py` puts `src/` directly on `sys.path`). When adding a new submodule, follow this same dual-import pattern or it will break one of the two run modes.

### Loan domain (`src/loan/`)
This is the core of the app and the file to budget context for — `crud.py` is ~5,300 lines. Key concepts:

- **`loan_type` parameter** (`kasbon` | `extradana` | `aku_cicil` | `installment` | `"all"`) is the recurring filter across nearly every endpoint, distinguishing three actual loan products by SQL predicate rather than a DB column:
  - `kasbon`/default "loan": `l.duration = 1 AND l.loan_id NOT IN (<AkuCicil ids>)`
  - `extradana`: `l.duration != 1 AND l.disbursement != 4 AND l.loan_id NOT IN (<AkuCicil ids>)`
  - `aku_cicil`: `l.loan_id IN (<AkuCicil ids>)` — AkuCicil loan ids come from `loan_setting` where `loan_type = 'AkuCicil'`, cached per-process in `_cached_aku_cicil_id_list` (module-level global, not per-request — reset requires process restart)
  - `installment`: extradana OR aku_cicil
  - `"all"`: union of all three (`ALL_LOAN_CONDITIONS`)
- **Employer/placement/project hierarchy** comes from three joins against `tbl_gmc` (a generic lookup/dimension table keyed by `group_gmc`): `sub_client` = employer, `placement_client` = sourced_to, `client_project` = project. All three joins additionally require `aktif = 'Yes' AND keterangan3 = 1`. This join pattern (`_LOAN_GMC_JOINS` / `_KARYAWAN_GMC_JOINS`) is duplicated across many query builders — reuse those constants rather than re-deriving the join.
  - `client_segment`/`product_type` filters go one level further through `tbl_project_management` (joined via `prj.id`) and then back into `tbl_gmc` again (group `segment` / a product-type group) for human-readable labels.
- **`ALLOWED_COMPANIES`/`COMPANY_FILTER`**: internal payroll and some loan aggregates restrict to the four Valdo-family legal entities (`PT Valdo Sumber Daya Mandiri`, `PT Valdo International`, `PT Toko Pandai`, `PT Valdo Solusi Integra`). Watch for whether a given query should be scoped to just these four vs. all clients.
- Most endpoints exist in three variants: a point-in-time summary, a `-monthly` breakdown (requires `start_date`/`end_date`, buckets by month via `src/loan/date_filters.py`), and sometimes a raw list. When adding a metric, check whether it needs the same three-way treatment for consistency with siblings (e.g. `loan-fees` / `loan-fees-monthly`, `loan-risk` / `loan-risk-monthly`).
- `src/loan/date_filters.py` normalizes `YYYY-MM-DD` query params to start/end-of-day datetime strings and appends the `BETWEEN`-style SQL clause; use `append_date_filters` rather than hand-rolling date SQL.

### Payroll domains (`src/external_payroll/`, `src/internal_payroll/`)
Structurally near-identical route/schema shapes (`total_payroll_disbursed`, `total_payroll_headcount`, `total_department_count`, `total_bpsjtk`, `total_kesehatan`, `total_pensiun`, `filters`, `monthly`, `department_summary`, `cost_owner_summary`), both querying `payroll_detail`/`payroll_header`. The distinguishing filter is internal payroll's `dept_id = 0` plus `valdo_inc` restricted to VI/VSDM (see `INTERNAL_PAYROLL_VALDO_INC_ALLOWED` in `src/internal_payroll/crud.py`); external payroll has no such `valdo_inc` restriction. `KARYAWAN_DEPT_CODE_LABELS` in `internal_payroll/crud.py` documents the `td_karyawan.dept_code` integer→label mapping (BFSI/Non-BFSI/Corporate/Outsource).

### AI domain (`src/ai/`)
Independent of the MySQL database — these endpoints call OpenAI (`resume_scorer.py`, `interview_scorer.py`), AWS Transcribe/S3 (`transcribe.py`), and HeyGen (`heygen.py`) directly. `src/ai/schemas.py` defines generic chat/analysis schemas that are largely unused by the actual routes in `router.py` (each route instead defines its own request/response `BaseModel`s inline). `url_fetch.py` is a shared helper for downloading a remote file by URL into a temp file (used when scoring a resume/interview from a URL instead of an uploaded file) — it validates scheme and caps download size at 500 MB; reuse it rather than adding another ad-hoc `urllib` download path.

## Working in this codebase

- No ORM models exist for the loan/payroll tables (`src/models.py` is a stub `Base` used only so `models.Base.metadata.create_all()` doesn't error at startup). Add new queries as raw SQL in the relevant `crud.py`, parameterized via SQLAlchemy `text()` bind params (`:name`), never by string-interpolating user input.
- Route handlers catch broad `Exception` and return a 200 response with `"status": "error"` and zeroed/empty fields rather than propagating an HTTP error status — match this convention for new loan/payroll endpoints unless there's a reason to diverge.
- When adding a filter to a loan endpoint, it typically needs threading through: the route's query params → the `crud` function signature → the SQL `WHERE`/join clause → (if applicable) the `/loan/filters` endpoint's available-values response.

## Database Query Rules

- **SELECT** → langsung jalankan, tidak perlu konfirmasi.
- **Apapun yang mengubah data/skema** (INSERT, UPDATE, DELETE, ALTER, DROP, TRUNCATE, dan perintah Artisan yang berdampak sama seperti `migrate`, `migrate:fresh`, `migrate:rollback`, `migrate:refresh`, `db:seed`) → **wajib konfirmasi ke user terlebih dahulu** sebelum dieksekusi. Ini berlaku baik dijalankan lewat MCP MySQL, `php artisan tinker`, maupun `php artisan` langsung.
- Aturan ini berlaku untuk semua agent/subagent di `.claude/agents/` yang punya akses Bash atau ke database — bukan hanya sesi utama.

# Database Safety Rules

Database integrity is critical. Never perform write operations without explicit user approval.

## Allowed Without Confirmation

The following operations are considered read-only and may be executed immediately:

- `SELECT`
- `EXPLAIN`
- `SHOW`
- `DESCRIBE` (`DESC`)

These queries do not modify data and do **not** require user confirmation.

---

## Require Explicit User Confirmation

The following operations **must never be executed automatically**.

Always explain what will happen and obtain explicit user confirmation before proceeding.

- `INSERT`
- `UPDATE`
- `DELETE`
- `REPLACE`
- `UPSERT`
- `MERGE`
- `TRUNCATE`
- `ALTER`
- `DROP`
- `CREATE`
- `RENAME`
- Any stored procedure, function, or script that modifies data or schema
- Any SQL statement that may directly or indirectly modify database contents

If there is any uncertainty about whether a query is read-only or modifies data, **assume it is destructive and ask for confirmation first**.

---

## Best Practices

- Never delete production data without explicit approval.
- Never modify database schema without explicit approval.
- Prefer transactions for multi-step data modifications whenever possible.
- Explain the impact of destructive operations before execution.
- When appropriate, recommend taking a backup before performing irreversible changes.

# Git Workflow & Commit Guidelines

## Commit Rules

- **Never add `Co-Authored-By: Claude` (or any AI attribution) to commit or merge messages.**
- The repository owner is **moxiebagas**. All commits must appear as authored solely by **moxiebagas**.
- Never use `--author` or any mechanism that changes or injects another author into the Git history.
- Write **clear, descriptive, and informative commit messages** in **English**, following GitHub's best practices (e.g. Conventional Commits when applicable).

Examples:

```text
feat: add employee loan approval workflow

fix: prevent duplicate payroll generation on concurrent requests

refactor: simplify attendance validation logic

docs: update API authentication documentation
```

---

## Common Git Commands

```bash
# Push to story branch
git push origin story/VHRIS-3177

# Merge story → production (only when user explicitly approves)
git checkout production && git pull origin production
git merge story/VHRIS-3177 --no-ff -m "Merge branch 'story/VHRIS-3177' into production"
git push origin production
```


# Development Workflow

Before starting **every new task**, always make sure your branch is created from the latest `main`.

## 1. Check your current branch

If you are **not currently on `main`**, switch back to `main` first.

```bash
git checkout main
```

## 2. Update main

```bash
git pull origin main
```

## 3. Create a new task branch

```bash
git checkout -b task/your-task-name
```

Never create a new task branch from another feature branch.

Always create it from the latest `main`.

---

# Commit Changes

After completing the task:

```bash
git add <file> [<file> ...]   # stage by name: tracked src/**/__pycache__/*.pyc files are always modified noise
git commit -m "feat: implement payroll validation improvements"
```

Commit messages must:

- Be written in English
- Clearly explain what changed
- Follow GitHub/Conventional Commit style whenever possible

---

# Push Branch

Push the task branch to the remote repository.

```bash
git push -u origin task/your-task-name
```

---

# Pull Request

Open a Pull Request with:

- **Source branch:** `task/your-task-name`
- **Target branch:** `main`

The Pull Request title should clearly summarize the implemented change.

---

# Merge

After the Pull Request is approved (or when appropriate), merge it into `main`.

Use a merge commit when possible to preserve branch history.

Example:

```bash
git checkout main
git pull origin main

git merge task/your-task-name --no-ff

git push origin main
```

---

# After Merge

After the merge is completed:

1. Return to `main`

```bash
git checkout main
```

2. Pull the latest changes

```bash
git pull origin main
```

This ensures that **every new task always starts from the latest version of `main`**.

---

# Summary Workflow

```text
Checkout main
        ↓
Pull latest main
        ↓
Create task/new-feature
        ↓
Develop
        ↓
Commit (English, informative)
        ↓
Push task branch
        ↓
Open Pull Request → main
        ↓
Merge into main
        ↓
Checkout main
        ↓
Pull latest main
        ↓
Start next task from main
```

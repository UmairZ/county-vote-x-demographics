# County Vote × Demographics — Build Guide

> **This is a build guide, not the project README.** You're going to write the
> pipeline. When it works, replace this file with a real portfolio README —
> writing that up is part of the exercise.

---

## What you're building

A cross-matched county-level dataset: **MIT Election Lab presidential returns
joined to Census ACS demographics on 5-digit county FIPS**, on BigQuery,
transformed with dbt, orchestrated with Dagster.

The sentence you get to write afterward — and defend in an interview:

> Built a dbt + BigQuery pipeline cross-matching county election returns with
> Census demographics, orchestrated with Dagster; idempotent incremental loads
> and dbt tests, including referential checks that flag geographies failing to
> cross-match.

That covers BigQuery, dbt, Dagster, idempotency, and cross-matching in one line.

**The join is the easy part.** A FIPS join is thirty seconds of SQL. What makes
this worth building is the *quality gate around* the join — cross-matching
geographies fails in specific, recurring, boring ways, and the test that names
them is the thing you'll actually get asked about. Budget your weekend
accordingly: TODO 5 matters more than TODO 4.

---

## Target graph

```
Census ACS API ──► raw_census_acs5 ─────► stg_census ────┐
                                                          ├──► county_vote_x_demographics
Dataverse CSV ───► raw_county_president ─► stg_election ─┘         (incremental MERGE)
```

---

## What's already done vs. what's yours

| Done for you | Why |
|---|---|
| `pyproject.toml`, `uv.lock` | Dependency pinning isn't the lesson. Python is pinned to 3.12 — dbt and Dagster don't support 3.14 yet, which is what your system Python is. |
| `dbt/dbt_project.yml`, `dbt/profiles.yml` | Boilerplate. Read them anyway — `profiles.yml` pulls the project ID from env vars so no credentials land in git. |
| `dbt/models/staging/staging.yml` → `sources:` | Wiring. |
| **`dbt/models/staging/stg_election.sql`** | **Your worked example.** Complete and heavily commented. Read it before writing anything else — it demonstrates the CTE structure, the FIPS padding idiom, and one genuinely nasty grain bug. |

| Yours | File | Roughly |
|---|---|---|
| TODO 1 | `pipeline/etl.py` | 1 hr |
| TODO 2 | *(setup — see below)* | 30 min |
| TODO 3 | `dbt/models/staging/stg_census.sql` | 45 min |
| TODO 4 | `dbt/models/marts/county_vote_x_demographics.sql` | 1 hr |
| **TODO 5** | **`dbt/models/staging/staging.yml`** | **1 hr — the important one** |
| TODO 6 | `dbt/models/marts/marts.yml` | 20 min |
| TODO 7 | `dbt/tests/dem_two_party_share_in_range.sql` | 20 min |
| TODO 8 | `pipeline/definitions.py` | 1–2 hrs (Sunday) |

Every stub carries its spec, its traps, and a verification step inline. Work
the TODO numbers in order.

---

## Setup (TODO 2 — do this first, it has waiting in it)

### 1. Google Cloud, from zero (~15 min)

BigQuery's free **sandbox will not work.** It blocks the DML that incremental
`MERGE` needs, and it fails with a permissions error that never mentions
billing. Create the project with billing enabled. At this data size (a few MB)
you stay inside the free tier and pay effectively nothing.

1. Account at <https://console.cloud.google.com> — new accounts get free credit.
2. Create a project. Note the **project ID**, not the display name.
3. Billing → link a billing account **to that project**.
4. APIs & Services → enable **BigQuery API**.
5. Install gcloud: <https://cloud.google.com/sdk/docs/install>, then:

```powershell
gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID
```

### 2. Census API key (~2 min)

<https://api.census.gov/data/key_signup.html> — free, instant, arrives by email.

### 3. Election CSV — manual, and it has to be

Download **County Presidential Election Returns 2000–2024** from the
[Harvard Dataverse dataset page](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/VOQCHQ)
and drop `countypres_2000-2024.csv` into `data/raw/`.

Don't burn time trying to script this — I already did. Dataverse sits behind an
AWS WAF bot challenge: the REST API, curl, and a plain GET all return an empty
`202 Accepted` with `x-amzn-waf-action: challenge`. There's no scripted
download without solving a JS challenge.

### 4. Environment

```powershell
Copy-Item .env.example .env    # fill it in
Get-Content .env | ForEach-Object { $p = $_ -split '=', 2; [Environment]::SetEnvironmentVariable($p[0], $p[1]) }

uv sync --python 3.12
```

Verify before writing code — this should succeed on the scaffold as-is:

```powershell
cd dbt
uv run dbt parse
uv run dbt debug     # confirms BigQuery auth actually works
```

---

## Working loop

```powershell
cd dbt
uv run dbt build --select stg_census          # one model
uv run dbt build                              # everything + tests
uv run dbt build --full-refresh               # rebuild the incremental mart
uv run dbt show --select stg_census --limit 5 # eyeball output without leaving the terminal
```

From the repo root: `uv run python -m pipeline.etl` runs extract + load.

The stubs contain `select 1 as replace_me` so the project parses from the
start — that way `dbt debug` proves your credentials before you've written any
SQL. Replace those lines entirely.

---

## Scope it in two passes

**Saturday — the core, and enough on its own.** TODO 1–7. BigQuery project,
both CSVs loaded, staging + mart, the tests, a real README. That alone
legitimately claims BigQuery + dbt + cross-matching + data-quality testing.

**Sunday — the upgrades.** TODO 8 (Dagster), then make the mart incremental
(TODO 4d) and prove the incremental path with a second election year. This is
what earns "orchestrated" and "idempotent" honestly.

If Sunday disappears, Saturday still ships. Don't start TODO 8 before TODO 5
is green.

---

## Traps I already hit, so you don't lose hours to them

| Trap | What happens |
|---|---|
| **Python 3.14** | dbt and Dagster don't support it. Already pinned to 3.12 in `pyproject.toml` — use `uv run`, not bare `python`. |
| **Census API without a key** | Silently 302s to an HTML "Missing Key" page, so `json.load()` dies with a parse error that says nothing about credentials. |
| **BigQuery sandbox** | Incremental `MERGE` fails with a permissions error that doesn't mention billing. Enable billing. |
| **FIPS leading zeros** | BigQuery autodetect reads FIPS as INT64 and eats the zero. `01001` becomes `1001`, and every Alabama county fails to cross-match. Fix it in dbt, not the loader. |
| **ACS jam values** | Missing estimates come back as `-666666666`, not NULL. They look like real numbers and quietly poison any average. |
| **Vote modes** | Some states report a `TOTAL` row *alongside* per-mode rows. A plain `SUM` double-counts those states. See `stg_election.sql` — this is the bug TODO 7's test is aimed at. |
| **dbt 1.11 test syntax** | Generic test arguments now nest under `arguments:`. Older tutorials show them at the top level; that still runs but warns. |
| **Dagster asset keys** | dbt source keys are `["raw", "table"]`. A bare `@asset` won't match, and you'll get two disconnected graphs instead of lineage. See TODO 8b. |

---

## Be ready to explain these

The build is the means; these are the deliverable.

1. **Why the relationships test warns instead of erroring** — and what number you picked for `error_if`, and why. Have the actual list of flagged geographies from your run.
2. **Two layers of idempotency** — `WRITE_TRUNCATE` at the load, `MERGE` on a unique key at the mart. Why you need both.
3. **Why the watermark is `>=` and not `>`** — late-arriving certification amendments, and what `--full-refresh` is for.
4. **Why the mart joins `INNER`** — you're deliberately trading silent row loss for a clean table, and paying for it with a test.
5. **Why `dbt build` rather than `run` then `test`.**
6. **The vintage mismatch** — one ACS 5-year window joined against returns spanning 2000–2024. Naming a limitation of your own dataset unprompted is a strong signal.

---

## Checking your work

A complete reference implementation is on the **`reference`** branch.

Use it as a check, not a source. The value of this exercise is entirely in
having written it yourself — an interviewer will ask *why* you made a choice,
and reading someone else's answer doesn't get you there.

```powershell
git diff reference -- dbt/models/staging/stg_census.sql   # after you've written yours
git show reference:pipeline/etl.py                        # if you're properly stuck
```

Delete the branch before making the repo public: `git branch -D reference`

---

## Data sources

- [MIT Election Data and Science Lab, *County Presidential Election Returns 2000–2024*](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/VOQCHQ) — CC0
- [U.S. Census Bureau, ACS 5-Year Estimates Detailed Tables](https://www.census.gov/data/developers/data-sets/acs-5year.html) — public domain

Both are public and contain no PII.

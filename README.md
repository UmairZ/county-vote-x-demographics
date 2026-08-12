# County Vote × Demographics

A county-level dataset joining MIT Election Lab presidential returns (2000–2024) to Census ACS demographics on 5-digit county FIPS. Built on BigQuery, transformed with dbt, orchestrated with Dagster.

The join is straightforward. The part I spent most of the time on is the test around it, which asserts that every election FIPS resolves to a Census county. It currently flags 50 geographies that do not match, all of them real quirks of US geography rather than pipeline bugs.

All data is public and contains no PII.

---

## The graph

```
Census ACS API ──► raw_census_acs5 ─────► stg_census ────┐
                                                          ├──► county_vote_x_demographics
Dataverse CSV ───► raw_county_president ─► stg_election ─┘        (incremental MERGE)
```

| Layer | Object | Rows | What it does |
|---|---|---|---|
| raw | `raw_county_president` | 94,151 | Copy of the MEDSL CSV |
| raw | `raw_census_acs5` | 3,222 | Copy of the ACS API response |
| staging | `stg_election` (view) | 44,164 | Zero-pads FIPS, filters to two-party presidential, collapses vote modes |
| staging | `stg_census` (view) | 3,222 | Rebuilds FIPS from `state`+`county`, casts estimates, nulls jam values |
| mart | `county_vote_x_demographics` (table) | 21,743 | The join, plus two-party share and demographic percentages |

Mart grain: one row per county per election year per office.

---

## Notes on the data

**50 geographies fail to cross-match.** The `relationships` test flags 678 rows across 50 FIPS codes, written to an audit table by `store_failures: true`.

| FIPS | State | Reason |
|---|---|---|
| `02001`–`02040` | AK | Returns are reported by state house district under pseudo-FIPS. These overlap numerically with real borough codes (`02013` is a genuine Aleutians East Borough code), so a bad join could produce false matches, not just missing ones. |
| `09001`–`09015` | CT | All eight counties, every year. Connecticut's counties were replaced by nine planning regions in the 2022+ ACS. The boundaries do not nest. |
| `29380`, `36000` | MO | Kansas City spans four counties and is reported separately. Its code changed between the 2020 and 2024 vintages. |
| `46113` | SD | Shannon County became Oglala Lakota County in 2015 and moved to `46102`. The source follows that through 2020, then reverts to `46113` in 2024. |
| `51515` | VA | Bedford City was dissolved in 2013 and its FIPS retired. Present in returns through 2016, absent from the ACS. |

The Oglala Lakota case is why I used a test rather than a static crosswalk: the source's coding changed after any crosswalk would have been written.

**Vote modes double-count if summed naively.** The source uses 20 different `mode` labels, and some states report a `TOTAL` row alongside the per-mode rows that sum to it.

```
county-year-party groups:
    TOTAL row only              40,200
    per-mode rows only           2,450
    BOTH (double-count risk)     1,514
```

`SUM(candidatevotes)` doubles the votes in those 1,514 groups without erroring or failing any null or uniqueness check. `stg_election` uses the `TOTAL` row where one exists and sums the modes where it does not.

**ACS uses negative sentinels for suppressed estimates**, not nulls. Two counties out of 3,222 carried `-666666666`, which is enough to move the average county median household income from $65,047 to -$348,815. `stg_census` nulls anything negative, testing the property rather than listing known sentinel codes.

---

## Tests

Sixteen tests, run interleaved with the models by `dbt build`, so a failure stops its own downstream.

The cross-match gate:

```yaml
config:
  severity: error
  warn_if: ">0"
  error_if: ">1000"
  store_failures: true
```

Thresholds come from the measured baseline. Known drift is 678 rows and warns. A structural break (lost zero-padding, wrong ACS vintage, wrong join key) misses thousands at once and errors.

I checked this by removing the `LPAD` from `stg_election` on purpose. Failures went to 4,624 and the build failed with seven downstream nodes skipped. The first version of this config did not fail: `severity: warn` caps the outcome and makes `error_if` unreachable.

| Test | What it protects |
|---|---|
| `unique`, `not_null` on `stg_census.county_fips` | The join. A duplicate FIPS fans out and doubles vote totals. |
| `accepted_values` on `party` | The two-party denominator, if the `WHERE` clause is ever loosened. |
| `unique`, `not_null` on `county_year_office_key` | The `MERGE`. A bad merge duplicates rows, which looks like plausible data. |
| `dem_two_party_share_in_range` | Shares outside `[0,1]`, which would mean upstream double-counting. |
| `census_estimates_not_jammed` | Regression test for the sentinel values above. |

---

## Idempotency

Loads use `WRITE_TRUNCATE`. Both sources are full snapshots (the Census API returns every county, the MEDSL file contains every year), so there is no incremental subset to load and replacing is simpler. The default `WRITE_APPEND` would double the table on a retry.

The mart is incremental with `unique_key='county_year_office_key'`, so re-runs `MERGE` rather than append. Three consecutive builds:

```
before:               21,743 rows,  21,743 distinct keys
after two more runs:  21,743 rows,  21,743 distinct keys
```

The watermark is `>=` rather than `>` so the newest cycle is reprocessed each run, since that is the one amended after certification. Backfilling an older year needs `--full-refresh`.

---

## Example output

2024 Democratic two-party share by county education quintile:

```
    Q1  13.4% BA+  ->  26.0% Dem
    Q5  40.5% BA+  ->  48.0% Dem
```

Most- and least-educated county quintiles over time:

```
    2000    44.3% Dem  vs  43.9% Dem
    2024    48.0% Dem  vs  26.0% Dem
```

One caveat: this uses a single ACS vintage (2023 5-year) against elections spanning 2000–2024, so demographics are held fixed and only the vote changes. The second table shows how currently high-education counties voted over time, not how counties shifted as they became more educated. Doing that properly would need a matching ACS vintage per election year.

---

## Running it

**Prerequisites.** A billing-enabled GCP project; the free BigQuery sandbox blocks the DML that incremental `MERGE` needs and fails with a permissions error that does not mention billing. A [Census API key](https://api.census.gov/data/key_signup.html), free and instant. The [gcloud CLI](https://cloud.google.com/sdk/docs/install), authenticated with `gcloud auth application-default login`.

**The election CSV is a manual download.** Harvard Dataverse sits behind an AWS WAF bot challenge; the REST API, curl, and a plain GET all return an empty `202 Accepted` with `x-amzn-waf-action: challenge`. Download *County Presidential Election Returns 2000–2024* from [the dataset page](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/VOQCHQ), choosing **Original File Format** rather than the `.tab` ingest, and put it in `data/raw/`. `pipeline/etl.py` looks for it and fails with instructions if it is missing.

```powershell
Copy-Item .env.example .env    # fill in GCP_PROJECT and CENSUS_API_KEY
. .\env.ps1                    # dot-sourced, so dbt and Dagster see the vars too
uv sync --python 3.12
```

Orchestrated:

```powershell
uv run dagster dev             # UI with lineage at localhost:3000
```

or headless:

```powershell
uv run dagster asset materialize -m pipeline.definitions `
  --select "raw/raw_county_president,raw/raw_census_acs5,stg_election,stg_census,county_vote_x_demographics"
```

The two Python loaders run in parallel and `dbt build` waits for both. That edge exists because the assets use `key_prefix="raw"`, which gives them the same asset keys `dagster-dbt` derives from the dbt sources, so no custom translator is needed.

Piecemeal:

```powershell
uv run python -m pipeline.etl        # extract + load
cd dbt
uv run dbt build                     # models + tests
uv run dbt build --full-refresh      # rebuild the incremental mart
```

---

## Stack notes

Python is pinned to 3.12; dbt and Dagster do not support 3.14. The Census API requires a key on every request and answers unkeyed ones with an HTML "Missing Key" page served as HTTP 200, so a status-code check passes and JSON parsing then fails with an error that says nothing about credentials. MEDSL writes missing values as the string `NA`, which autodetect types as `INT64` and then chokes on; `null_marker="NA"` turns those 89 rows into real NULLs instead of dropping them with `max_bad_records`.

FIPS zero-padding is handled in dbt rather than at load. BigQuery autodetect reads `01001` as the integer `1001`, and the Census API's separate `state` and `county` columns suffer the same fate. `LPAD(CAST(x AS STRING), 5, '0')` is correct whether the column arrives as `INT64` or `STRING`, so the fix lives in one place and cannot drift out of sync with the loader.

---

## Data sources

- [MIT Election Data and Science Lab, *County Presidential Election Returns 2000–2024*](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/VOQCHQ), CC0
- [U.S. Census Bureau, ACS 5-Year Estimates Detailed Tables](https://www.census.gov/data/developers/data-sets/acs-5year.html), public domain

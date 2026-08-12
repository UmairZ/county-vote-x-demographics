"""Extract Census ACS, and load both raw CSVs into BigQuery.

Raw stays a faithful copy of the source; all cleaning happens in dbt.
Loading twice leaves BigQuery in the same state as loading once.
"""

import csv
import json
import os
import urllib.parse
import urllib.request
from pathlib import Path

from google.cloud import bigquery

RAW = Path(__file__).resolve().parent.parent / "data" / "raw"

ACS_VARS = [
    "NAME",
    "B01003_001E",  # total population
    "B19013_001E",  # median household income
    "B01002_001E",  # median age
    "B15003_001E",  # education universe: population 25+
    "B15003_022E",  # bachelor's
    "B15003_023E",  # master's
    "B15003_024E",  # professional degree
    "B15003_025E",  # doctorate
    "B03002_001E",  # race/ethnicity universe
    "B03002_003E",  # white, not hispanic
    "B03002_004E",  # black, not hispanic
    "B03002_012E",  # hispanic, any race
]


def extract_census(year: int = 2023) -> Path:
    """Pull the ACS 5-year county table to data/raw/. Return the CSV path.

    The API needs a key on every request and answers unkeyed ones with an HTML
    "Missing Key" page served as HTTP 200, so a status check passes and JSON
    parsing then fails with an error that says nothing about credentials. The
    guard below turns that into something legible.
    """
    key = os.environ.get("CENSUS_API_KEY")
    if not key:
        raise RuntimeError("CENSUS_API_KEY is not set.")
    joined = ",".join(ACS_VARS)
    params = {"get": joined, "for": "county:*", "key": key}
    query  = urllib.parse.urlencode(params)
    url    = f"https://api.census.gov/data/{year}/acs/acs5?" + query

    with urllib.request.urlopen(url, timeout=120) as resp:
        rows = json.load(resp)   # [header, *rows]; state and county are appended

    out = RAW / f"census_acs5_{year}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as fh:
        csv.writer(fh).writerows(rows)

    return out


def election_csv() -> Path:
    """Find the MEDSL county returns CSV in data/raw/. Return its path.

    A lookup rather than a download. Harvard Dataverse sits behind an AWS WAF
    bot challenge that answers every non-browser client with an empty 202, so
    the file is fetched by hand once and this only has to locate it.
    """
    hits = sorted(RAW.glob("countypres_*.csv"))
    if not hits:
        raise FileNotFoundError(
            f"No countypres_*.csv in {RAW}. Download 'County Presidential Election "
            "Returns 2000-2024' from https://dataverse.harvard.edu/dataset.xhtml"
            "?persistentId=doi:10.7910/DVN/VOQCHQ"
        )
    return hits[-1]


def load_bigquery(csv_path: Path, table: str) -> str:
    """Load one CSV into <GCP_PROJECT>.<BQ_DATASET>.<table>. Return the full ref.

    WRITE_TRUNCATE is what makes this idempotent. Both sources are full
    snapshots, so replacing the table each run is correct and a re-run is a
    no-op. The default WRITE_APPEND would silently double the rows.

    Autodetect reads the FIPS columns as INT64 and drops the leading zeros.
    That is left alone on purpose: dbt pads them back, so the models are right
    whether the column arrives as INT64 or STRING.
    """
    project = os.environ["GCP_PROJECT"]
    dataset = os.environ.get("BQ_DATASET", "elections")
    client = bigquery.Client(project=project)

    ds = bigquery.Dataset(f"{project}.{dataset}")
    ds.location = os.environ.get("BQ_LOCATION", "US")
    client.create_dataset(ds, exists_ok=True)

    ref = f"{project}.{dataset}.{table}"
    with csv_path.open("rb") as fh:
        job = client.load_table_from_file(
            fh,
            ref,
            job_config=bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                autodetect=True,
                # MEDSL writes missing values as the string "NA", in
                # county_fips on non-county rollup rows and in candidatevotes.
                # Declaring the marker turns those 89 rows into real NULLs.
                # Without it autodetect types the column INT64 and the load
                # dies on the first "NA".
                null_marker="NA",
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            ),
        )
    job.result()   # load jobs are async; this blocks and raises on failure
    return ref


if __name__ == "__main__":
    print(load_bigquery(election_csv(), "raw_county_president"))
    print(load_bigquery(extract_census(), "raw_census_acs5"))

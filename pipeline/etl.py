"""Extract Census ACS + load both raw CSVs into BigQuery.

TODO 1 -- fill in the three functions below.

The design rule for this layer: the raw tables should be a faithful copy of
the source, and loading them twice should leave BigQuery in the same state as
loading them once. All cleaning happens in dbt, all incremental logic happens
in the dbt mart. Keep this file boring.
"""

import csv
import json
import os
import urllib.parse
import urllib.request
from pathlib import Path

from google.cloud import bigquery

RAW = Path(__file__).resolve().parent.parent / "data" / "raw"

# Given to you -- looking these up is tedious, not educational.
# Percentages get derived in dbt, so only raw counts are pulled here.
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

    TODO 1a.

    Endpoint shape:
        https://api.census.gov/data/{year}/acs/acs5?get=<vars>&for=county:*&key=<key>

    `for=county:*` alone returns every county nationwide -- you do not need
    to loop over states.

    The response is JSON: a list of lists, where row 0 is the header. Two
    extra columns, `state` and `county`, are appended beyond what you asked
    for -- those are the two halves of the FIPS code. Write the whole thing
    straight to CSV; do not reshape it here.

    GOTCHA -- the Census API requires a key for multi-variable requests, and
    it does not tell you politely. An unkeyed request 302s to an HTML page
    titled "Missing Key", so json.load() dies with a confusing parse error
    rather than anything about credentials. Read CENSUS_API_KEY from the
    environment and raise a clear error yourself if it is missing.
    Free and instant: https://api.census.gov/data/key_signup.html

    Write to: RAW / f"census_acs5_{year}.csv"
    """
    raise NotImplementedError


def election_csv() -> Path:
    """Locate the MEDSL county returns CSV in data/raw/.

    TODO 1b -- and note what this function does NOT do.

    You would expect to download this. You cannot. Harvard Dataverse sits
    behind an AWS WAF bot challenge: the Dataverse REST API, curl, and a
    plain GET all come back as an empty `202 Accepted` carrying the header
    `x-amzn-waf-action: challenge`. There is no scripted download without
    solving a JS challenge, so the file is fetched by hand once and this
    function only has to find it.

    Glob RAW for "countypres_*.csv". If nothing matches, raise
    FileNotFoundError with the Dataverse URL in the message -- a scaffold
    that fails with instructions is worth more than one that fails with a
    stack trace.

    Worth saying out loud in an interview: knowing which integrations are
    genuinely un-automatable, and handling that honestly instead of leaving
    a fetch that silently returns empty, is the actual skill being tested.
    """
    raise NotImplementedError


def load_bigquery(csv_path: Path, table: str) -> str:
    """Load one CSV into <GCP_PROJECT>.<BQ_DATASET>.<table>. Return the full ref.

    TODO 1c.

    Read GCP_PROJECT, BQ_DATASET and BQ_LOCATION from the environment. Create
    the dataset if it does not exist (`exists_ok=True`), then load the file
    with `client.load_table_from_file`.

    THE IDEMPOTENCY BIT -- this is half of the claim you get to make later.
    Set write_disposition to WRITE_TRUNCATE. The source is a full snapshot,
    so replacing the table on every run means re-running is a no-op instead
    of doubling your rows. The default is WRITE_APPEND, which is exactly the
    bug. (The other half of the claim is the MERGE in the dbt mart.)

    Also set: source_format=CSV, skip_leading_rows=1, autodetect=True.

    Do not fight autodetect over the FIPS columns. It reads them as INT64 and
    eats the leading zero, and that is fine -- the dbt staging models pad them
    back. Handling it there means the models are correct whether the column
    arrives as INT64 or STRING, which is one less thing to keep in sync.

    Remember to call job.result() -- load jobs are async, and without it you
    will report success before BigQuery has finished (or failed).
    """
    raise NotImplementedError


if __name__ == "__main__":
    print(load_bigquery(election_csv(), "raw_county_president"))
    print(load_bigquery(extract_census(), "raw_census_acs5"))

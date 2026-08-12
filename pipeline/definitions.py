"""Dagster asset graph: extract and load, then dbt build (models and tests).

The two Python assets use key_prefix="raw", which makes their asset keys
["raw", "raw_county_president"] and ["raw", "raw_census_acs5"]. Those are the
same keys dagster-dbt derives from the dbt sources in staging.yml, so matching
the naming is what connects the two halves of the graph. No custom
DagsterDbtTranslator needed.
"""

from pathlib import Path

from dagster import AssetExecutionContext, Definitions, asset
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from pipeline.etl import election_csv, extract_census, load_bigquery

dbt_project = DbtProject(project_dir=Path(__file__).resolve().parent.parent / "dbt")

# Regenerates the manifest under `dagster dev` so new models appear without a
# manual `dbt parse`. Reads profiles.yml, which reads env vars, so this fails
# at import time if GCP_PROJECT is unset.
dbt_project.prepare_if_dev()


@asset(key_prefix="raw", compute_kind="python")
def raw_county_president() -> str:
    """MEDSL county returns CSV to BigQuery. WRITE_TRUNCATE, so re-runs are flat."""
    return load_bigquery(election_csv(), "raw_county_president")


@asset(key_prefix="raw", compute_kind="python")
def raw_census_acs5() -> str:
    """Census ACS 5-year API to BigQuery. WRITE_TRUNCATE."""
    return load_bigquery(extract_census(), "raw_census_acs5")


@dbt_assets(manifest=dbt_project.manifest_path)
def dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    # `build` rather than `run` then `test`. build tests each model right after
    # building it, so a failing test stops its own downstream instead of
    # letting a bad mart get consumed while the suite catches up.
    yield from dbt.cli(["build"], context=context).stream()


defs = Definitions(
    assets=[raw_county_president, raw_census_acs5, dbt_models],
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
)

"""Dagster asset graph: extract/load -> dbt build (models + tests).

TODO 8 -- Sunday's work. Get the dbt project running standalone first.

This is what earns the word "orchestrated." Right now you have two things
that happen to run in sequence because you type two commands. An asset graph
is a declared dependency structure -- Dagster knows stg_election cannot run
before raw_county_president, and can show you that as lineage.
"""

from pathlib import Path

from dagster import AssetExecutionContext, Definitions, asset
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets

from pipeline.etl import election_csv, extract_census, load_bigquery

# TODO 8a: point DbtProject at the ../dbt directory, then call
# .prepare_if_dev() on it.
#
# prepare_if_dev() runs `dbt parse` to regenerate the manifest when you are
# running `dagster dev`. Without it, Dagster reads a stale manifest and your
# new models do not appear until you manually re-parse.
#
# NOTE: dbt parse reads profiles.yml, which reads env vars. If GCP_PROJECT
# is unset, `dagster dev` fails at import time with a dbt error that does
# not obviously mention the environment. Load your .env first.
dbt_project = ...


# TODO 8b: two Python assets wrapping the loaders in etl.py.
#
#   raw_county_president  -> load_bigquery(election_csv(), "raw_county_president")
#   raw_census_acs5       -> load_bigquery(extract_census(), "raw_census_acs5")
#
# THE ONE THING THAT MATTERS HERE -- asset key naming.
#
# dagster-dbt derives an asset key for every dbt source, and the key it
# derives is ["<source_name>", "<table_name>"]. Your sources live under
# `name: raw` in staging.yml, so dbt's keys are:
#
#     ["raw", "raw_county_president"]
#     ["raw", "raw_census_acs5"]
#
# A bare @asset named raw_county_president gets the key
# ["raw_county_president"] -- which does NOT match, so Dagster shows you two
# disconnected graphs and no lineage. The usual fix people reach for is a
# custom DagsterDbtTranslator subclass overriding get_asset_key(). You do
# not need one. Look at @asset's `key_prefix` argument and make the keys
# line up by naming instead.
#
# Add compute_kind="python" too -- it just labels the node in the UI.


# TODO 8c: the dbt assets.
#
#   @dbt_assets(manifest=<the project's manifest_path>)
#   def dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
#       yield from dbt.cli([...], context=context).stream()
#
# Which dbt command? The instinct is "run", then a separate node for "test".
# Prefer `build`. dbt build tests each model immediately after building it,
# so a failing test halts its own downstream instead of letting a bad mart
# get consumed while the test suite is still catching up. One node, stricter
# semantics.


# TODO 8d: wire it together.
#
#   defs = Definitions(
#       assets=[...],
#       resources={"dbt": DbtCliResource(project_dir=dbt_project)},
#   )
#
# VERIFY: `uv run dagster dev`, open localhost:3000, and look at the graph.
# You are checking for FIVE assets in ONE connected graph:
#
#   raw/raw_county_president  ->  stg_election  ->\
#                                                  county_vote_x_demographics
#   raw/raw_census_acs5       ->  stg_census    ->/
#
# If you see six or seven assets in two disconnected clumps, TODO 8b's key
# naming is off. That is the bug this file is really about.

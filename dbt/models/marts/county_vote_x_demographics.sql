-- =====================================================================
-- TODO 4 -- county_vote_x_demographics    <- this is the deliverable
--
-- The "foundational cross-matched dataset." Everything else exists to
-- feed this table and to prove it is correct.
--
-- GRAIN: one row per county per election year per office.
--
-- ---------------------------------------------------------------------
-- STEP 4a -- pivot the votes
--
--   stg_election is one row per county-year-office-PARTY. The mart needs
--   one row per county-year-office, with dem and rep side by side.
--
--   Aggregate stg_election into a CTE producing:
--     county_fips, election_year, office, state_po, county_name,
--     dem_votes, rep_votes
--
--   The pivot is a conditional sum: sum(if(party = 'DEMOCRAT', ...)).
--   county_name needs any_value() -- it is not in the group by.
--
-- STEP 4b -- join to demographics
--
--   inner join stg_census on county_fips.
--
--   Note what INNER means here: any county that fails to cross-match
--   vanishes without a trace. That silent loss is exactly what TODO 5's
--   relationships test exists to catch. Use inner anyway -- a mart with
--   half-null demographic rows is worse -- but understand that you are
--   deliberately trading silence for cleanliness, and paying for it with
--   a test.
--
-- STEP 4c -- compute
--
--   county_year_office_key  STRING   concat(fips, '-', year, '-', office)
--   two_party_votes         INT64    dem + rep
--   dem_two_party_share     FLOAT64  dem / (dem + rep)
--   pct_bachelors_plus      FLOAT64  pop_bachelors_plus / pop_25_plus
--   pct_white_nh            FLOAT64  pop_white_nh / pop_race_universe
--   pct_black_nh            FLOAT64  pop_black_nh / pop_race_universe
--   pct_hispanic            FLOAT64  pop_hispanic / pop_race_universe
--
--   ...plus the passthrough columns: county_fips, election_year, office,
--   state_po, county_name, dem_votes, rep_votes, census_geo_name,
--   total_population, median_household_income, median_age.
--
--   Use safe_divide(), not `/`. A county with zero two-party votes is a
--   real row in this data, and `/` will fail the whole build on it.
--
-- ---------------------------------------------------------------------
-- STEP 4d -- MAKE IT INCREMENTAL   (Sunday. Get it working as a table first.)
--
--   Uncomment the config block at the bottom of this file. Note that it is
--   wrapped in Jinja comment markers {# ... #}, not SQL `--` markers, and
--   you have to delete those too.
--
--   (Worth knowing why: Jinja is templated BEFORE the SQL is ever parsed,
--   so a config block sitting behind SQL `--` markers is not commented out
--   at all -- dbt still evaluates it, and you get a compile error from a
--   line you thought was inert. SQL comments cannot hide Jinja; only Jinja
--   comment markers can. I hit this writing this very file.)
--
--   materialized='incremental' + unique_key means dbt generates a MERGE
--   instead of an INSERT, so re-running updates rows in place rather than
--   appending duplicates. That is the whole idempotency claim.
--
--   Then add a watermark inside the votes CTE:
--
--       {% raw %}{% if is_incremental() %}
--       where election_year >= (
--           select coalesce(max(election_year), 0) from {{ this }}
--       )
--       {% endif %}{% endraw %}
--
--   Use >=, not >. The newest cycle is the one that gets amended after
--   certification, so you want to reprocess it every run and let the
--   MERGE absorb the correction. > would freeze the first load forever.
--   (Backfilling an OLDER year then needs --full-refresh. That trade is
--   worth being able to explain out loud.)
--
--   PREREQ: BigQuery sandbox blocks the DML that MERGE needs. Billing
--   must be enabled on the project or this step fails with a permissions
--   error that does not mention billing.
--
--   PROVE IT (this is the demo worth rehearsing):
--     1. Load the CSV filtered to year <= 2020, dbt build, note row count
--     2. Load the full CSV, dbt build again
--     3. Row count grew by exactly the 2024 rows -- nothing duplicated
--     4. dbt build a third time -- row count does not move at all
--
-- =====================================================================

{#
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='county_year_office_key',
        on_schema_change='sync_all_columns'
    )
}}
#}

select 1 as replace_me

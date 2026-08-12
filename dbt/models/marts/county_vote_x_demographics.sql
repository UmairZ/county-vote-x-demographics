-- County-level two-party presidential vote joined to ACS demographics on
-- 5-digit county FIPS.
--
-- Grain: one row per county per election year per office.
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='county_year_office_key',
        on_schema_change='sync_all_columns'
    )
}}

with votes as (

    -- stg_election is one row per county-year-office-party. The mart needs dem
    -- and rep side by side, so this pivots with conditional sums and drops the
    -- grain one level to county-year-office.
    select
        county_fips,
        election_year,
        office,
        state_po,
        any_value(county_name)                            as county_name,
        sum(if(party = 'DEMOCRAT',   candidate_votes, 0)) as dem_votes,
        sum(if(party = 'REPUBLICAN', candidate_votes, 0)) as rep_votes

    from {{ ref('stg_election') }}

    {% if is_incremental() %}
    -- Watermark is >=, not >, so the newest cycle is reprocessed every run.
    -- That is the one amended after certification, and the MERGE absorbs the
    -- correction. With > it would freeze at the first load. Backfilling an
    -- older year needs --full-refresh. {{ this }} is this model's own table.
    where election_year >= (
        select coalesce(max(election_year), 0) from {{ this }}
    )
    {% endif %}

    group by county_fips, election_year, office, state_po

)

select
    concat(v.county_fips, '-', cast(v.election_year as string), '-', v.office)
                                                    as county_year_office_key,
    v.county_fips,
    v.election_year,
    v.office,
    v.state_po,
    v.county_name,

    v.dem_votes,
    v.rep_votes,
    v.dem_votes + v.rep_votes                       as two_party_votes,

    -- safe_divide rather than `/`. A county with zero two-party votes is a
    -- real row here, and plain division would fail the build on it.
    safe_divide(v.dem_votes, v.dem_votes + v.rep_votes)
                                                    as dem_two_party_share,

    c.geo_name                                      as census_geo_name,
    c.total_population,
    c.median_household_income,
    c.median_age,
    safe_divide(c.pop_bachelors_plus, c.pop_25_plus)       as pct_bachelors_plus,
    safe_divide(c.pop_white_nh,       c.pop_race_universe) as pct_white_nh,
    safe_divide(c.pop_black_nh,       c.pop_race_universe) as pct_black_nh,
    safe_divide(c.pop_hispanic,       c.pop_race_universe) as pct_hispanic

-- INNER join, so any county that fails to cross-match drops out silently. The
-- relationships test in staging.yml is what surfaces that. Inner is still the
-- right choice, since a mart of half-null demographic rows is worse, but it is
-- a trade paid for with a test.
from votes v
inner join {{ ref('stg_census') }} c
    on v.county_fips = c.county_fips

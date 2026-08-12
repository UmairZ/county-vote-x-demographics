-- Regression test for the ACS sentinel values.
--
-- ACS encodes suppressed and not-applicable estimates as negative numbers
-- (-666666666 and relatives) rather than nulls. Two counties in the 2023
-- vintage carried one, enough to move the national average county income from
-- $65,047 to -$348,815.
--
-- stg_census nulls anything negative. This asserts that worked, and that a
-- sentinel the cleaning does not anticipate gets caught rather than averaged.

select
    county_fips,
    geo_name,
    total_population,
    median_household_income,
    median_age
from {{ ref('stg_census') }}
where median_household_income <= 0
   or median_age              <= 0
   or total_population        <  0

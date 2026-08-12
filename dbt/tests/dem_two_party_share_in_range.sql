-- A two-party share outside [0, 1] is arithmetically impossible. It would mean
-- votes were double-counted upstream or went negative.
--
-- Aimed specifically at the vote-mode collapse in stg_election: 1,514
-- county-year-party groups report a TOTAL row and the per-mode rows that sum
-- to it. If that CASE expression is ever simplified to a plain SUM, those
-- groups double and this fires.
--
-- Selects the vote columns too, not just the key, so a failure says what went
-- wrong rather than only that something did.

select
    county_year_office_key,
    dem_votes,
    rep_votes,
    two_party_votes,
    dem_two_party_share
from {{ ref('county_vote_x_demographics') }}
where dem_two_party_share is null
   or dem_two_party_share < 0
   or dem_two_party_share > 1

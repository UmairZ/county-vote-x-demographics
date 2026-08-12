-- =====================================================================
-- TODO 7 -- a singular test
--
-- Generic tests (not_null, unique, relationships) are reusable and live
-- in YAML. A SINGULAR test is just a SQL file that selects the rows that
-- should not exist. If it returns zero rows it passes. That is the whole
-- contract.
--
-- Write a query against ref('county_vote_x_demographics') that returns
-- any row where dem_two_party_share is null, below 0, or above 1.
--
-- Select enough columns to actually debug it -- the key, dem_votes,
-- rep_votes, and the share itself. A test that tells you something is
-- wrong without telling you what is half a test.
--
-- WHAT THIS ACTUALLY CATCHES:
--   A share above 1 means votes were double-counted upstream -- which is
--   precisely the TOTAL-vs-per-mode trap that stg_election.sql handles.
--   If someone "simplifies" that CASE expression into a plain SUM, this
--   test is what fires. Read the comment in stg_election.sql to see the
--   bug this is aimed at.
-- =====================================================================

select 1 as replace_me
where false

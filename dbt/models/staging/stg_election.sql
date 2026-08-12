with src as (

    select
        cast(year as int64)                                as election_year,
        upper(trim(office))                                as office,
        upper(trim(party))                                 as party,
        upper(trim(mode))                                  as vote_mode,
        upper(trim(state_po))                              as state_po,
        trim(county_name)                                  as county_name,
        cast(candidatevotes as int64)                      as candidate_votes,
        -- The load autodetects county_fips as INT64 (leading zero eaten) or as
        -- STRING depending on the vintage of the file. CAST-then-LPAD is correct
        -- for both, which is why the zero-padding lives here and not in the load.
        lpad(cast(county_fips as string), 5, '0')          as county_fips
    from {{ source('raw', 'raw_county_president') }}
    where county_fips is not null   -- statewide / "uncounted" rollup rows

)

select
    election_year,
    office,
    county_fips,
    state_po,
    any_value(county_name) as county_name,
    party,

    -- Grain fix, not cosmetics: several states report a `TOTAL` row *alongside*
    -- per-mode rows (ELECTION DAY / ABSENTEE / PROVISIONAL) for the same county.
    -- Summing everything double-counts those states, so prefer TOTAL where it
    -- exists and fall back to summing the modes where it does not.
    case
        when countif(vote_mode = 'TOTAL') > 0
            then sum(if(vote_mode = 'TOTAL', candidate_votes, 0))
        else sum(candidate_votes)
    end as candidate_votes

from src
where office = 'US PRESIDENT'
  and party in ('DEMOCRAT', 'REPUBLICAN')   -- two-party denominator
group by election_year, office, county_fips, state_po, party

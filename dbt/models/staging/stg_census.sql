with cast_cols as (

    select
        -- The API returns state and county as separate columns ('01', '001')
        -- and the load autodetects both as INT64, so both lose their leading
        -- zeros. Pad each half to its own width before concatenating.
        concat(
            lpad(cast(state as string), 2, '0'),
            lpad(cast(county as string), 3, '0')
        )                                                     as county_fips,
        name                                                  as geo_name,

        cast(B01003_001E as int64)                            as total_population,
        cast(B19013_001E as int64)                            as median_household_income,
        cast(B01002_001E as float64)                          as median_age,

        cast(B15003_001E as int64)                            as pop_25_plus,
        cast(B15003_022E as int64) + cast(B15003_023E as int64)
          + cast(B15003_024E as int64) + cast(B15003_025E as int64)
                                                              as pop_bachelors_plus,

        cast(B03002_001E as int64)                            as pop_race_universe,
        cast(B03002_003E as int64)                            as pop_white_nh,
        cast(B03002_004E as int64)                            as pop_black_nh,
        cast(B03002_012E as int64)                            as pop_hispanic

    from {{ source('raw', 'raw_census_acs5') }}

)

select
    county_fips,
    geo_name,

    -- ACS encodes suppressed and not-applicable estimates as negative
    -- sentinels (-666666666 and relatives) rather than nulls. Two counties in
    -- the 2023 vintage carry one, which is enough to move the national average
    -- county income from $65,047 to -$348,815. Null anything negative, testing
    -- the property rather than listing known codes.
    if(total_population        < 0, null, total_population)        as total_population,
    if(median_household_income < 0, null, median_household_income) as median_household_income,
    if(median_age              < 0, null, median_age)              as median_age,

    -- Counts and universes are left alone; ACS does not jam these the same way.
    pop_25_plus,
    pop_bachelors_plus,
    pop_race_universe,
    pop_white_nh,
    pop_black_nh,
    pop_hispanic

from cast_cols

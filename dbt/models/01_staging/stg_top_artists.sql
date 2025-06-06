{{ config(materialized = 'view') }}

with source as (

    select *
    from {{ source('raw', 'RAW_TOP_ARTISTS') }}

), cleaned as (

    select
        ID::string         as ARTIST_ID,
        NAME::string       as ARTIST_NAME,
        POPULARITY::number as ARTIST_POPULARITY,
        FOLLOWERS::number  as ARTIST_FOLLOWERS,

        -- Keep genres as-is, let downstream handle it
        GENRES as ARTIST_GENRES,

        {{ current_timestamp() }} as _loaded_at
    from source

), dedup as (

    select *
    from cleaned
    qualify row_number() over (
        partition by ARTIST_ID
        order by _loaded_at desc
    ) = 1

)

select * from dedup
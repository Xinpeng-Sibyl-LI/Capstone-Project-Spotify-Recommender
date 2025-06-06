{{ config(materialized = 'view') }}

with source as (

    select *
    from {{ source('raw', 'RAW_TOP_TRACKS') }}  -- Updated table name

), cleaned as (

    select
        ID::string            as track_id,       -- Updated column name
        NAME::string          as track_name,     -- Updated column name
        POPULARITY::number    as track_popularity, -- Updated column name
        ARTIST_ID::string     as artist_id,      -- Updated column name

        /* album name lives inside json_data column */
        JSON_DATA:"album":"name"::string as album_name, -- Updated column name

        DURATION_MS::number   as duration_ms,    -- Updated column name
        EXPLICIT::boolean     as is_explicit,    -- Updated column name

        {{ current_timestamp() }} as _loaded_at
    from source

), dedup as (

    select *
    from cleaned
    qualify row_number() over (
        partition by track_id
        order by _loaded_at desc
    ) = 1

)

select * from dedup
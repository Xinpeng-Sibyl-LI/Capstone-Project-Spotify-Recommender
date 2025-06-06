{{ config(materialized = 'view') }}

with source as (
    select *
    from {{ source('raw', 'RAW_LISTENING_HISTORY') }}  -- Updated table name
    where PLAY_ID is not null  -- Updated column name (likely uppercase)
),

cleaned as (
    select
        PLAY_ID::string as play_id,      -- Snowflake columns often uppercase
        USER_ID::string as user_id,
        TRACK_ID::string as track_id,
        ARTIST_ID::string as artist_id,
        
        -- Simple timestamp conversion
        PLAY_TS::timestamp_ntz as play_ts,  -- Updated column name

        CURRENT_TIMESTAMP AS ingestion_ts,

        -- Handle genres without complex type checking
        case
            when GENRES is null then array_construct()
            when is_array(GENRES) then GENRES
            else array_construct()
        end as play_genres,
        
        upper(LANGUAGE::string) as language,
        initcap(DEVICE::string) as device,
        
        {{ current_timestamp() }} as _loaded_at
    from source
    where PLAY_ID is not null  -- Updated column name
)

select * from cleaned
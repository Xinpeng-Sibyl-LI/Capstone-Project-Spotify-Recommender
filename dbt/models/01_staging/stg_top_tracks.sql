{{ config(materialized='view') }}

with source as (
    select *
    from {{ source('raw', 'RAW_TOP_TRACKS') }}
),

cleaned as (
    select
        ID::string as track_id,
        NAME::string as track_name,
        POPULARITY::number as track_popularity,
        ARTIST_ID::string as artist_id,
        DURATION_MS::number / 1000.0 as duration_seconds,
        EXPLICIT::boolean as is_explicit,
        TRACK_LANGUAGE::string as track_language
    from source
)

select * from cleaned
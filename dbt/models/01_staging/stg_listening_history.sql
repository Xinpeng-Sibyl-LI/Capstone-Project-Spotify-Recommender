{{ config(materialized='view') }}

with source as (
    select *
    from {{ source('raw', 'RAW_LISTENING_HISTORY') }}
    where PLAY_ID is not null
),

cleaned as (
    select
        PLAY_ID::string as play_id,
        USER_ID::string as user_id,
        TRACK_ID::string as track_id,
        ARTIST_ID::string as artist_id,
        PLAY_TS::timestamp_ntz as play_ts,
        TRACK_LANGUAGE::string as track_language,
        DEVICE::string as device,
        PLAY_DURATION_SECONDS::number as play_duration_seconds,
        SKIPPED::boolean as skipped
    from source
)

select * from cleaned
{{ config(materialized='view') }}

with source as (
    select *
    from {{ source('raw', 'RAW_TOP_ARTISTS') }}
),

cleaned as (
    select
        ID::string as artist_id,
        NAME::string as artist_name,
        POPULARITY::number as artist_popularity,
        FOLLOWERS::number as artist_followers,
        GENRES as artist_genres
    from source
)

select * from cleaned
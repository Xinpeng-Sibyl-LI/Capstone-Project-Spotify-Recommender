{{ config(materialized='view') }}

with source as (

    select * 
    from {{ source('raw', 'raw_top_tracks') }}

), renamed as (

    select
        id                      as track_id,
        name                    as track_name,
        popularity              as track_popularity,
        artist_ids[0]::varchar  as artist_id,     -- first artist in array
        album ->> 'name'        as album_name,
        {{ current_timestamp() }} as _loaded_at
    from source

)

select * from renamed;

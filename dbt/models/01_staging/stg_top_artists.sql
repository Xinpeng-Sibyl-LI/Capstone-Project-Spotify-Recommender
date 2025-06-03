{{ config(materialized='view') }}

with source as (

    select * 
    from {{ source('raw', 'raw_top_artists') }}

), renamed as (

    select
        id          as artist_id,
        name        as artist_name,
        popularity  as artist_popularity,
        followers   as artist_followers,
        genres      as artist_genres,
        {{ current_timestamp() }} as _loaded_at
    from source

)

select * from renamed;

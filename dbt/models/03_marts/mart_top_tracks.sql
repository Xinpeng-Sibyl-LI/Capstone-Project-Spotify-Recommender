{{ config(materialized='table') }}

with source as (

    select *
    from {{ ref('int_tracks_with_artists') }}
    where _loaded_at = (select max(_loaded_at) from {{ ref('int_tracks_with_artists') }})

), renamed as (

    select
        *,
        row_number() over (order by track_popularity desc) as rn
    from source

)

select
    track_id,
    track_name,
    artist_id,
    artist_name,
    album_name,
    track_popularity,
    artist_followers,
    _loaded_at
from renamed
where rn <= 100;

{{ config(
    materialized='view'
) }}

with stg_top_tracks as (

    select * 
    from {{ ref('stg_top_tracks') }}

),

stg_top_artists as (

    select * 
    from {{ ref('stg_top_artists') }}

),

tracks_with_artists as (

    select
        t.track_id,
        t.track_name,
        t.track_popularity,
        t.album_name,
        a.artist_id,
        a.artist_name,
        a.artist_popularity,
        a.artist_followers,
        t._loaded_at
    from stg_top_tracks   as t
    left join stg_top_artists as a
        on t.artist_id = a.artist_id

),

final as (

    select * 
    from tracks_with_artists

)

select * from final;

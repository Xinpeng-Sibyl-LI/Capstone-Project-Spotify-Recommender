{{ config(materialized='table') }}

with source as (

    select *
    from {{ ref('int_tracks_with_artists') }}

), renamed as (

    select
        artist_id,
        artist_name,
        artist_followers,
        avg(track_popularity)  as avg_track_popularity,
        max(track_popularity)  as peak_track_popularity,
        count(*)               as num_tracks,
        max(_loaded_at)        as last_loaded_at
    from source
    group by artist_id, artist_name, artist_followers

)

select * from renamed;

{{ config(materialized='table') }}

with lhist as (
    select * from {{ ref('stg_listening_history') }}
),

tracks as (
    select * from {{ ref('stg_top_tracks') }}
),

artists as (
    select * from {{ ref('stg_top_artists') }}
),

joined as (
    select
        l.play_id,
        l.user_id,
        l.play_ts,
        l.device,
        l.language,
        l.play_genres,
        t.track_id,
        t.track_name,
        t.track_popularity,
        t.album_name,
        t.duration_ms,
        t.is_explicit,
        a.artist_id,
        a.artist_name,
        a.artist_popularity,
        a.artist_followers,
        a.artist_genres
    from lhist l
    left join tracks t on l.track_id = t.track_id
    left join artists a on l.artist_id = a.artist_id
)

select * from joined
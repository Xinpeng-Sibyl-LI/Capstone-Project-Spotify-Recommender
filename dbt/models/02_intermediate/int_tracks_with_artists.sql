{{ config(materialized='ephemeral') }}

with tracks as (
    select * from {{ ref('stg_top_tracks') }}
),

artists as (
    select * from {{ ref('stg_top_artists') }}
),

tracks_with_artists as (
    select
        t.track_id,
        t.track_name,
        t.track_popularity,
        t.track_language,
        t.duration_seconds,
        t.is_explicit,
        a.artist_id,
        a.artist_name,
        a.artist_popularity,
        a.artist_followers,
        a.artist_genres
    from tracks t
    left join artists a on t.artist_id = a.artist_id
),

-- Add deduplication in case of any duplicates from staging
deduped as (
    select *
    from tracks_with_artists
    qualify row_number() over (
        partition by track_id 
        order by track_popularity desc
    ) = 1
)

select * from deduped
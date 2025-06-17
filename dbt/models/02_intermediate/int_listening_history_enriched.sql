{{ config(materialized='ephemeral') }}

with listening_history as (
    select * from {{ ref('stg_listening_history') }}
),

tracks_with_artists as (
    select * from {{ ref('int_tracks_with_artists') }}
),

enriched as (
    select
        -- Play event details
        l.play_id,
        l.user_id,
        l.play_ts,
        l.device,
        l.play_duration_seconds,
        l.skipped,
        
        -- Track details
        l.track_id,
        t.track_name,
        t.track_popularity,
        t.track_language,
        t.duration_seconds,
        t.is_explicit,
        
        -- Artist details
        l.artist_id,
        t.artist_name,
        t.artist_popularity,
        t.artist_followers,
        t.artist_genres
        
    from listening_history l
    left join tracks_with_artists t on l.track_id = t.track_id
),

-- Add deduplication for any duplicate play events
deduped as (
    select *
    from enriched
    qualify row_number() over (
        partition by play_id 
        order by play_ts desc
    ) = 1
)

select * from deduped
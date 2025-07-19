{{ config(
    materialized='incremental',
    unique_key='track_id',
    on_schema_change='fail',
    incremental_strategy='merge'
) }}

with tracks_base as (
    select *
    from {{ ref('int_tracks_with_artists') }}
),

-- Get listening metrics from user history
listening_metrics as (
    select 
        track_id,
        count(*) as total_plays,
        count(distinct user_id) as unique_listeners,
        avg(play_duration_seconds) as avg_listen_duration,
        sum(case when skipped = true then 1 else 0 end) as total_skips,
        count(distinct to_date(play_ts)) as days_played
    from {{ ref('int_listening_history_enriched') }}
    group by track_id
),

track_summary as (
    select
        t.track_id,
        t.track_name,
        t.artist_id,
        t.artist_name,
        t.track_popularity,
        t.artist_popularity,
        t.duration_seconds,
        t.track_language,
        t.is_explicit,
        
        -- Listening metrics
        coalesce(l.total_plays, 0) as total_plays,
        coalesce(l.unique_listeners, 0) as unique_listeners,
        coalesce(l.avg_listen_duration, 0) as avg_listen_duration,
        coalesce(l.total_skips, 0) as total_skips,
        coalesce(l.days_played, 0) as days_played,
        
        -- Calculated metrics
        case 
            when l.total_plays > 0 then round(l.total_skips::float / l.total_plays * 100, 2)
            else 0 
        end as skip_rate_percent,
        
        case
            when l.unique_listeners > 0 then round(l.total_plays::float / l.unique_listeners, 2)
            else 0
        end as avg_plays_per_listener,
        
        case
            when t.duration_seconds > 0 then round(l.avg_listen_duration / t.duration_seconds * 100, 2)
            else 0
        end as completion_rate_percent
        
    from tracks_base t
    left join listening_metrics l on t.track_id = l.track_id
),

final as (
    select
        *,
        
        -- Use macro for popularity categorization
        {{ get_popularity_range('track_popularity') }} as popularity_range,
        
        -- Engagement tier based on actual listening behavior
        case 
            when total_plays >= 100 and skip_rate_percent <= 20 then 'High Engagement'
            when total_plays >= 50 and skip_rate_percent <= 40 then 'Moderate Engagement'
            when total_plays >= 10 then 'Low Engagement'
            else 'Minimal Engagement'
        end as engagement_tier,
        
        -- Track performance category
        case
            when completion_rate_percent >= 80 then 'Highly Compelling'
            when completion_rate_percent >= 60 then 'Compelling' 
            when completion_rate_percent >= 40 then 'Average Appeal'
            else 'Low Appeal'
        end as appeal_category,
        
        -- Language popularity (how popular is this track in its language category)
        rank() over (
            partition by track_language 
            order by track_popularity desc
        ) as language_popularity_rank
        
    from track_summary
)

select * from final
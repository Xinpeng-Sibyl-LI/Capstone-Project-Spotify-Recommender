{{ config(materialized='table') }}

with source as (

    select *
    from {{ ref('int_tracks_with_artists') }}

), artist_metrics as (

    select
        artist_id,
        artist_name,
        artist_popularity,
        artist_followers,
        
        -- Track-level aggregations
        avg(track_popularity) as avg_track_popularity,
        max(track_popularity) as max_track_popularity,
        min(track_popularity) as min_track_popularity,
        count(*) as num_tracks,
        
        -- Calculated metrics
        max(track_popularity) - min(track_popularity) as popularity_range,
        stddev(track_popularity) as track_popularity_stddev,
        
    from source
    group by 
        artist_id, 
        artist_name, 
        artist_popularity,
        artist_followers

), final as (

    select
        *,
        
        -- Add artist tiers based on followers
        case 
            when artist_followers >= 10000000 then 'Mega Star'
            when artist_followers >= 1000000 then 'Major Artist'
            when artist_followers >= 100000 then 'Popular Artist'
            when artist_followers >= 10000 then 'Emerging Artist'
            else 'New Artist'
        end as artist_tier,
        
        -- Track consistency score (lower stddev = more consistent popularity)
        case 
            when track_popularity_stddev <= 5 then 'Very Consistent'
            when track_popularity_stddev <= 10 then 'Consistent'
            when track_popularity_stddev <= 20 then 'Variable'
            else 'Highly Variable'
        end as consistency_rating
        
    from artist_metrics

)

select * from final
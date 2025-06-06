{{ config(
    materialized          = 'incremental',
    unique_key            = 'pk',
    incremental_strategy  = 'insert_overwrite',
    partition_by          = {'field': 'play_date', 'data_type': 'date'},
    on_schema_change      = 'sync_all_columns'
) }}

with max_ts as (
    {% if is_incremental() %}
        select
            coalesce(
                max(last_play_ts),
                '1900-01-01 00:00:00'::timestamp_ntz
            ) as last_ts
        from {{ this }}
        where last_play_ts is not null
          and last_play_ts < current_timestamp()
    {% else %}
        select '1900-01-01 00:00:00'::timestamp_ntz as last_ts
    {% endif %}
),

source as (
    select
        s.*
    from {{ ref('int_listening_history_enriched') }} s
    join max_ts on 1=1
    where s.play_ts > max_ts.last_ts - interval '2 hours'
      and s.play_ts is not null
      and s.user_id is not null
      and s.track_id is not null
),
-- - If running incrementally: only gets records where play_ts > (max play_ts from existing table - 2 hours)
-- - If running full refresh: returns 1=1 (gets all records)

per_day as (
    select
        user_id,
        to_date(play_ts)                   as play_date,
        count(*)                           as plays,
        count(distinct track_id)           as distinct_tracks,
        count(distinct artist_id)          as distinct_artists,

        -- Genre handling - simplified approach first
        max(
          case
            when play_genres is not null
             and array_size(play_genres) > 0
             and trim(play_genres[0]::string) != ''
            then play_genres[0]::string
            else null
          end
        )                                   as sample_genre,

        -- Time-based metrics
        min(play_ts)                       as first_play_ts,
        max(play_ts)                       as last_play_ts,
        datediff('second', min(play_ts), max(play_ts)) as listening_duration_seconds,

        -- Track metrics
        avg(track_popularity)              as avg_track_popularity,
        min(track_popularity)              as min_track_popularity,
        max(track_popularity)              as max_track_popularity,

        -- Data quality flags
        count(case when track_popularity is null then 1 end) as tracks_missing_popularity,
        count(case when play_genres is null or array_size(play_genres) = 0 then 1 end) as tracks_missing_genre,

        -- Primary key
        md5(user_id || '|' || to_date(play_ts))  as pk,
        
        -- Metadata
        current_timestamp()                as dbt_updated_at

    from source
    group by user_id, to_date(play_ts)
),

final as (
    select
        *,
        -- Additional calculated fields
        case 
            when listening_duration_seconds = 0 then 0
            else round(plays::float / (listening_duration_seconds / 3600.0), 2)
        end as plays_per_hour,
        
        case
            when distinct_tracks > 0 then round(plays::float / distinct_tracks, 2)
            else 0
        end as avg_plays_per_track,

        -- Data quality score (0-1, higher is better)
        case 
            when plays > 0 then
                round((1.0 - (tracks_missing_popularity + tracks_missing_genre)::float / (2.0 * plays)), 2)
            else 0
        end as data_quality_score

    from per_day
)

select * from final

-- Add data quality tests as comments for reference
-- Uncomment and adjust these for your dbt_project.yml or schema.yml

/*
Data Quality Tests to Add:

tests:
  - dbt_utils.expression_is_true:
      expression: "plays > 0"
  - dbt_utils.expression_is_true:
      expression: "distinct_tracks > 0"
  - dbt_utils.expression_is_true:
      expression: "first_play_ts <= last_play_ts"
  - dbt_utils.expression_is_true:
      expression: "data_quality_score >= 0 and data_quality_score <= 1"

Schema Tests:
  - unique: pk
  - not_null: [pk, user_id, play_date, plays]
  - accepted_values:
      column: primary_genre
      values: ['Pop', 'Rock', 'Hip-Hop', 'Electronic', 'Jazz', 'Unknown']  # Adjust as needed
*/
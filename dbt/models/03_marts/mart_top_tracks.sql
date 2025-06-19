{{ config(materialized='table') }}

with source as (

    select *
    from {{ ref('int_tracks_with_artists') }}

), ranked as (

    select
        *,
        row_number() over (order by track_popularity desc, artist_popularity desc) as popularity_rank
    from source

), final as (

    select
        track_id,
        track_name,
        artist_id,
        artist_name,
        track_popularity,
        artist_popularity,
        artist_followers,
        popularity_rank,
        {{ get_popularity_range('track_popularity') }} as popularity_range, 
        
    from ranked
    where popularity_rank <= 100

)

select * from final
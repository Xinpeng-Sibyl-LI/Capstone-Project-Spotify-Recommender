{{ config(materialized='table') }}

with source as (

    select *
    from {{ ref('int_tracks_with_artists') }}
    where _loaded_at = (select max(_loaded_at) from {{ ref('int_tracks_with_artists') }})

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
        album_name,
        track_popularity,
        artist_popularity,
        artist_followers,
        popularity_rank,
        _loaded_at,
        {{ get_popularity_range('track_popularity') }} as popularity_range, 
        
    from ranked
    where popularity_rank <= 100

)

select * from final
-- Check for tracks with missing artists
select t.artist_id, count(*) as track_count
from {{ ref('stg_top_tracks') }} t
left join {{ ref('stg_top_artists') }} a on t.artist_id = a.artist_id
where a.artist_id is null
group by t.artist_id
order by track_count desc
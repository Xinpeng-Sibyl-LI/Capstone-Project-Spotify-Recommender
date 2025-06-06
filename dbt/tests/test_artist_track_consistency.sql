-- tests/singular/test_artist_track_consistency.sql
-- Ensure that tracks are properly linked to their artists
-- This test fails if we find tracks with artist_ids that don't exist in our artist table

SELECT 
    t.track_id,
    t.track_name,
    t.artist_id,
    'Missing artist reference' as issue_type
FROM {{ ref('stg_top_tracks') }} t
LEFT JOIN {{ ref('stg_top_artists') }} a 
    ON t.artist_id = a.artist_id
WHERE a.artist_id IS NULL
  AND t.artist_id IS NOT NULL

UNION ALL

-- Also check for tracks without any artist assigned
SELECT 
    track_id,
    track_name,
    artist_id,
    'No artist assigned' as issue_type  
FROM {{ ref('stg_top_tracks') }}
WHERE artist_id IS NULL
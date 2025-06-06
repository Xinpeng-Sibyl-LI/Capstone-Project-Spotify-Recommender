-- Fail if plays < 1  OR  plays < distinct_tracks
SELECT
    user_id,
    play_date,
    plays,
    distinct_tracks
FROM {{ ref('mart_user_daily_plays') }}
WHERE plays < 1
   OR plays < distinct_tracks

--{{ ref('…') }} gives the compiled table name so the test works in every env.
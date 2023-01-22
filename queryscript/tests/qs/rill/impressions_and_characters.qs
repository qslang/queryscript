import * from schema;

-- This query originates from rill data's repo:
-- https://github.com/rilldata/rill-developer-example/blob/main/models/impressions_and_characters.sql
-- and uses CTEs, which I've replaced to just be variables.

-- This is some wacky duckdb syntax that should be pretty easy to support:
-- * Putting the FROM before the SELECT
-- * EXCLUDE wildcard
let Timeseries = (
    FROM episodes
    SELECT
    MAKE_DATE(
        CAST(STR_SPLIT(aired, ' ')[3] AS INT),
        CAST(STRFTIME(STRPTIME(STR_SPLIT(aired, ' ')[1],'%B'), '%-m') AS INT),
        CAST(REPLACE(STR_SPLIT(aired, ' ')[2], ',', '') AS INT)
        ) AS event_date,
    * EXCLUDE(aired)
);

-- We don't need to do the transformation that Rill does because we properly parse this file.
let ActorsTransformed = (
    SELECT * FROM actors
);

let CharactersOrImpressions = (
    FROM impressions SELECT *, 'impressions' AS category
    UNION
    FROM characters SELECT *, 'characters' AS category
);

-- This is the actual query
FROM Timeseries a
LEFT JOIN appearances b ON a.epid = b.epid
LEFT JOIN CharactersOrImpressions c ON b.role = c.name
LEFT JOIN ActorsTransformed d on b.aid = d.aid
SELECT
  a.event_date,
  a.sid AS season_id,
  CAST(a.sid AS VARCHAR) AS season_number,
  a.epid AS episode_id,
  CAST(epno AS VARCHAR) AS episode_number,
  b.tid AS appearance_id,
  CONCAT(
    'season:',CAST(a.sid AS VARCHAR),
    ' episode:', CAST(epno AS VARCHAR),
    ' segment:', CAST(tid AS VARCHAR)[9:]
    ) AS appearance_number,
  CAST(tid AS VARCHAR)[9:] AS appearance_number,
  b.aid AS actor,
  capacity AS actor_capacity,
  gender AS actor_gender,
  role AS actor_role,
  COALESCE(category, 'other') AS role_category,
  charid AS role_id,
  voice,

WHERE b.aid IS NOT NULL
;

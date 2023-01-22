import playoffs, teams from schema;

let normalized_games = unsafe
    SELECT
        TEAM_ID AS team,
        SEASON as season,
        strptime(GAME_DATE, '%Y-%m-%d') AS game_date,
        (SELECT TEAM_ID FROM playoffs i WHERE i.GAME_ID = o.GAME_ID and i.TEAM_ID != o.TEAM_ID) opponent,
        CASE WHEN WL='W' THEN 1 ELSE -1 END AS won
    from playoffs o;

let series = unsafe
    SELECT
        season,
        team,
        opponent,
        SUM(won) > 0 AS winner,
        MIN(game_date) as first_game
    FROM normalized_games
    GROUP BY 1, 2, 3
;

let series_ranked = unsafe
    select *, ROW_NUMBER() OVER (PARTITION BY (season, team) ORDER BY first_game) AS "round_number" FROM series;

let first_round_bye =
    SELECT
        series_ranked.season, series_ranked.team, series_ranked.round_number
    FROM series_ranked
    JOIN series_ranked o ON series_ranked.opponent = o.team AND o.opponent = series_ranked.team AND series_ranked.season = o.season
    WHERE
        series_ranked.round_number = 1 AND o.round_number = 2
;

-- NOTE: It would be super convenient to re-assign series_ranked here instead of defining a new variable name
-- NOTE: It would be nice to do series_ranked.*, some formula as round_number  (instead of listing out the rest of the fields)
let series_ranked_with_bye = unsafe
    SELECT
        series_ranked.season, series_ranked.team, series_ranked.opponent, series_ranked.winner, series_ranked.first_game,
        series_ranked.round_number + (CASE WHEN first_round_bye.team IS NULL THEN 0 ELSE 1 END) AS round_number
    FROM series_ranked LEFT JOIN first_round_bye USING (season, team)
;

-- Let the 0th round be the "entry" to the playoffs, and the rest (e.g. 1st) be the winner.
let all_series = unsafe
    SELECT team, season, first_game, 0 as round_number FROM series_ranked_with_bye WHERE round_number = 1
    UNION ALL
    SELECT team, season, first_game, round_number FROM series_ranked_with_bye WHERE winner=true
;

-- NOTE: It would be great to somehow factor out the join on teams so it can be run on any view with
-- a team id (perhaps named team, or perhaps make that an argument too).

-- Teams that showed up in the playoffs the most
SELECT teams.full_name, total FROM (
    SELECT team, COUNT(*) total FROM all_series GROUP BY 1
) appearances JOIN teams ON appearances.team = teams.ID ORDER BY 2 DESC LIMIT 10;

-- Teams that showed up in the finals the most
unsafe SELECT teams.full_name, total FROM (
    SELECT team, COUNT(*) total FROM all_series
        WHERE round_number = (SELECT DISTINCT round_number FROM all_series ORDER BY round_number DESC LIMIT 1 OFFSET 1)
    GROUP BY 1
) appearances JOIN teams ON appearances.team = teams.ID ORDER BY 2 DESC LIMIT 10;

let winners = unsafe
    SELECT team, COUNT(*) total FROM all_series
        WHERE round_number = (SELECT DISTINCT round_number FROM all_series ORDER BY round_number DESC LIMIT 1)
    GROUP BY 1
;

-- Teams that won the most
SELECT teams.full_name, total FROM winners appearances JOIN teams ON appearances.team = teams.ID ORDER BY 2 DESC LIMIT 10;

-- Teams that showed up in the playoffs but never won
unsafe SELECT teams.full_name, total FROM (
    SELECT team, COUNT(*) total FROM all_series
        WHERE team NOT IN (SELECT team FROM winners)
    GROUP BY 1
) appearances JOIN teams ON appearances.team = teams.ID ORDER BY 2 DESC LIMIT 10;

-- Teams that showed up in the finals but never won
unsafe SELECT teams.full_name, total FROM (
    SELECT team, COUNT(*) total FROM all_series
        WHERE round_number = (SELECT DISTINCT round_number FROM all_series ORDER BY round_number DESC LIMIT 1 OFFSET 1)
        AND team NOT IN (SELECT team FROM winners)
    GROUP BY 1
) appearances JOIN teams ON appearances.team = teams.ID ORDER BY 2 DESC LIMIT 10;

SELECT round_number, COUNT(DISTINCT team) teams FROM all_series GROUP BY 1 ORDER BY 1;

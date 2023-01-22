import playoffs, teams from schema;

let normalized_games =
    SELECT
        TEAM_ID AS team,
        SEASON as season,
        strptime(GAME_DATE, '%Y-%m-%d') AS game_date,
        (SELECT TEAM_ID FROM playoffs i WHERE i.GAME_ID = o.GAME_ID and i.TEAM_ID != o.TEAM_ID) opponent,
        CASE WHEN WL='W' THEN 1 ELSE -1 END AS won
    from playoffs o;

let series = SELECT
        season,
        team,
        opponent,
        SUM(won) > 0 AS winner,
        MIN(game_date) as first_game
    FROM normalized_games
    GROUP BY ALL
;

let series_ranked = unsafe
    select *, ROW_NUMBER() OVER (PARTITION BY (season, team) ORDER BY first_game) AS "round_number" FROM series
;

let first_round_bye =
    SELECT
        series_ranked.season, series_ranked.team, series_ranked.round_number
    FROM series_ranked
    JOIN series_ranked o ON series_ranked.opponent = o.team AND o.opponent = series_ranked.team AND series_ranked.season = o.season
    WHERE
        series_ranked.round_number = 1 AND o.round_number = 2
;

-- NOTE: It would be super convenient to re-assign series_ranked here instead of defining a new variable name
let series_ranked_with_bye =
    SELECT
        series_ranked.* EXCLUDE (round_number),
        series_ranked.round_number + (CASE WHEN first_round_bye.team IS NULL THEN 0 ELSE 1 END) AS round_number
    FROM series_ranked LEFT JOIN first_round_bye USING (season, team)
;

-- Let the 0th round be the "entry" to the playoffs, and the rest (e.g. 1st) be the winner.
let all_series =
    SELECT * EXCLUDE (round_number), 0 as round_number FROM series_ranked_with_bye WHERE round_number = 1
    UNION ALL
    SELECT * FROM series_ranked_with_bye WHERE winner=true
;

-- How do we denote this will only have 1 row?
fn full_name(team) {
    SELECT full_name FROM teams WHERE team = teams.ID LIMIT 1
}

fn n_to_last_round(n) {
    SELECT DISTINCT round_number FROM all_series ORDER BY round_number DESC LIMIT 1 OFFSET n
}

-- Teams that showed up in the playoffs the most
SELECT full_name(team)) full_name, COUNT(*) total FROM all_series GROUP BY 1 ORDER BY 2 DESC LIMIT 10;

-- Teams that showed up in the finals the most
SELECT full_name(team) full_name, COUNT(*) total FROM all_series
    WHERE round_number = n_to_last_round(1)
GROUP BY 1 ORDER BY 2 DESC LIMIT 10;

fn is_winner(team) {
    team IN (SELECT DISTINCT team FROM all_series WHERE round_number = n_to_last_round(0))
}

-- Teams that won the most
SELECT full_name(team), total FROM winners ORDER BY 2 DESC LIMIT 10;

-- Teams that showed up in the playoffs but never won
SELECT full_name(team), COUNT(*) total FROM all_series
    WHERE NOT is_winner(team)
GROUP BY 1 ORDER BY 2 DESC LIMIT 10;

-- Teams that showed up in the finals but never won
SELECT full_name(team), COUNT(*) total FROM all_series
    WHERE round_number = n_to_last_round(1)
    AND NOT is_winner(team)
GROUP BY 1 ORDER BY 2 DESC LIMIT 10;

-- Overall funnel
SELECT round_number, COUNT(DISTINCT team) teams FROM all_series GROUP BY 1 ORDER BY 1;

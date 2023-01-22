import * from schema;

export fn day_to_round(day bigint) {
    CASE
        WHEN day = 136 or day = 137 THEN 'Round 1: 64'
        WHEN day = 138 or day = 139 THEN 'Round 2: 32 teams'
        WHEN day = 143 or day = 144 THEN 'Round 3: Sweet Sixteen'
        WHEN day = 145 or day = 146 THEN 'Round 4: Elite Eight'
        WHEN day = 152 THEN 'Round 5: Final Four'
        WHEN day = 154 THEN 'Round 6: National Final'
    END
}

-- NOTE: When we fix GROUP BY ordering, we should update this query?
SELECT DISTINCT "DayNum" from ncaa_tourney_compact_results ORDER BY "DayNum";

let tournament_stages =
    SELECT * FROM (
        SELECT ncaa_tourney_compact_results.*, teams.*, team_coaches."CoachName", day_to_round("DayNum") stage
            FROM ncaa_tourney_compact_results
            JOIN teams ON ncaa_tourney_compact_results."WTeamID" = teams."TeamID"
            JOIN team_coaches ON ncaa_tourney_compact_results."Season" = team_coaches."Season" and ncaa_tourney_compact_results."WTeamID" = team_coaches."TeamID"
    ) WHERE stage is not NULL;

SELECT * FROM (
    tournament_stages
) sub WHERE stage is not NULL ORDER BY "Season", "DayNum", "WTeamID", "LTeamID" LIMIT 10;

SELECT stage, COUNT(DISTINCT "TeamName") FROM tournament_stages GROUP BY 1 ORDER BY 1;
SELECT stage, COUNT(DISTINCT "CoachName") FROM tournament_stages GROUP BY 1 ORDER BY 1;

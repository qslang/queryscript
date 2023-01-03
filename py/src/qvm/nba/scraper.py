# From https://github.com/swar/nba_api/blob/master/docs/nba_api/stats/endpoints/leaguegamelog.md
from nba_api.stats.endpoints import leaguegamelog


for season in range(2000, 2022):
    print(season)
    gamefinder = leaguegamelog.LeagueGameLog(
        season=season,
        season_type_all_star="Regular Season",
        counter=0,
        direction="ASC",
        player_or_team_abbreviation="T",
        sorter="DATE",
        league_id="00",
    )

import os
from enum import Enum
from functools import partial

import pandas as pd
from nba_api.stats.endpoints import leaguegamelog, playoffpicture
from nba_api.stats.static import players, teams

from ..config import get_logger


logger = get_logger("nba-scraper")

CACHE_ENABLED = True


def disable_cache():
    global CACHE_ENABLED
    CACHE_ENABLED = False


class SeasonType(Enum):
    PLAYOFFS = "Playoffs"
    REGULAR_SEASON = "Regular Season"


class League(Enum):
    NBA = "00"
    ABA = "01"
    WNBA = "10"
    G_LEAGUE = "20"


class Conference(Enum):
    EAST = "east"
    WEST = "west"

    def offset(self):
        return 0 if self == Conference.EAST else 1


def cache_key(fn, *args, **kwargs):
    return "_".join([fn.__name__] + [str(x) for x in args] + [str(x) for x in kwargs.values()]) + ".parquet"


def save_to_cache(df, key, tmp_dir):
    cache_path = os.path.join(tmp_dir, key)
    df.to_parquet(cache_path)


def nba_cache(fn):
    def wrapper(tmp_dir, *args, **kwargs):
        key = cache_key(fn, *args, **kwargs)
        cache_path = os.path.join(tmp_dir, key)
        if os.path.exists(cache_path):
            logger.info(f"Loading from cache: {cache_path}")
            return pd.read_parquet(cache_path)
        else:
            logger.info(f"Scraping: {key}")
        ret = fn(*args, tmp_dir=tmp_dir, **kwargs)
        save_to_cache(ret, key, tmp_dir)
        return ret

    return wrapper


def scrape_playoff_picture_all(season: int, tmp_dir: str):
    data = playoffpicture.PlayoffPicture(league_id="00", season_id=f"2{season}")
    dfs = data.get_data_frames()

    for conference in [Conference.EAST, Conference.WEST]:
        save_to_cache(dfs[conference.offset()], cache_key(scrape_playoff_picture, season, conference), tmp_dir)
        save_to_cache(dfs[2 + conference.offset()], cache_key(scrape_playoff_standings, season, conference), tmp_dir)
        save_to_cache(
            dfs[4 + conference.offset()], cache_key(scrape_playoff_remaining_games, season, conference), tmp_dir
        )

    return dfs


@nba_cache
def scrape_playoff_picture(season: int, conference: Conference, tmp_dir: str):
    return scrape_playoff_picture_all(season, tmp_dir)[conference.offset()]


@nba_cache
def scrape_playoff_standings(season: int, conference: Conference, tmp_dir: str):
    base = 0 if conference == Conference.EAST else 2
    return scrape_playoff_picture_all(season, tmp_dir)[2 + conference.offset()]


@nba_cache
def scrape_playoff_remaining_games(season: int, conference: Conference, tmp_dir: str):
    base = 0 if conference == Conference.EAST else 2
    return scrape_playoff_picture_all(season, tmp_dir)[4 + conference.offset()]


@nba_cache
def scrape_season_games(season_type: SeasonType, season: int, tmp_dir: str):
    # https://github.com/swar/nba_api/blob/master/docs/nba_api/stats/endpoints/leaguegamelog.md
    data = leaguegamelog.LeagueGameLog(
        season=season,
        season_type_all_star=season_type.value,
        counter=0,
        direction="ASC",
        player_or_team_abbreviation="T",
        sorter="DATE",
        league_id=League.NBA.value,
    )
    return data.get_data_frames()[0]


def scrape_teams():
    return pd.DataFrame.from_records(teams.get_teams())


def scrape_players():
    return pd.DataFrame.from_records(players.get_players())


SCRAPERS = {
    "playoffs": partial(scrape_season_games, season_type=SeasonType.PLAYOFFS),
    "regular_season": partial(scrape_season_games, season_type=SeasonType.REGULAR_SEASON),
    "playoff_picture_east": partial(scrape_playoff_picture, conference=Conference.EAST),
    "playoff_picture_west": partial(scrape_playoff_picture, conference=Conference.WEST),
    "playoff_standings_east": partial(scrape_playoff_picture, conference=Conference.EAST),
    "playoff_standings_west": partial(scrape_playoff_picture, conference=Conference.WEST),
}


def add_season(df, season):
    df["SEASON"] = season
    return df


def scrape(start_season: int, end_season: int, datasets: set, tmp_dir: str, out_dir: str):
    scrape_teams().to_parquet(os.path.join(out_dir, "teams.parquet"))
    scrape_players().to_parquet(os.path.join(out_dir, "players.parquet"))

    for ds in datasets:
        logger.info(f"-- {ds}")
        fn = SCRAPERS[ds]
        all_seasons = pd.concat(
            [add_season(fn(season=season, tmp_dir=tmp_dir), season) for season in range(start_season, end_season + 1)]
        )
        all_seasons.to_parquet(os.path.join(out_dir, f"{ds}.parquet"))

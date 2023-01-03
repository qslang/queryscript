import os
from enum import Enum
from functools import partial

import pandas as pd
from nba_api.stats.endpoints import leaguegamelog

from ..config import get_logger


logger = get_logger("nba-scraper")


class SeasonType(Enum):
    PLAYOFFS = "Playoffs"
    REGULAR_SEASON = "Regular Season"


class League(Enum):
    NBA = "00"
    ABA = "01"
    WNBA = "10"
    G_LEAGUE = "20"


def nba_cache(fn):
    def wrapper(*args, tmp_dir=None, **kwargs):
        key = "_".join([fn.__name__] + [str(x) for x in args] + [str(x) for x in kwargs.values()]) + ".parquet"
        cache_path = os.path.join(tmp_dir, key)
        if os.path.exists(cache_path):
            logger.debug(f"Loading from cache: {key}")
            return pd.read_parquet(cache_path)
        ret = fn(*args, **kwargs)
        ret.to_parquet(cache_path)
        return ret

    return wrapper


@nba_cache
def scrape_season(season_type: SeasonType, season: int, tmp_dir=None):
    # https://github.com/swar/nba_api/blob/master/docs/nba_api/stats/endpoints/leaguegamelog.md
    gamefinder = leaguegamelog.LeagueGameLog(
        season=season,
        season_type_all_star=season_type.value,
        counter=0,
        direction="ASC",
        player_or_team_abbreviation="T",
        sorter="DATE",
        league_id=League.NBA.value,
    )
    return gamefinder.get_data_frames()[0]


SCRAPERS = {
    "playoffs": partial(scrape_season, season_type=SeasonType.PLAYOFFS),
    "regular_season": partial(scrape_season, season_type=SeasonType.REGULAR_SEASON),
}


def scrape(start_season: int, end_season: int, datasets: set, tmp_dir: str, out_dir: str):
    for ds in datasets:
        fn = SCRAPERS[ds]
        all_seasons = pd.concat([fn(season=season, tmp_dir=tmp_dir) for season in range(start_season, end_season + 1)])
        all_seasons.to_parquet(os.path.join(out_dir, f"{ds}.parquet"))

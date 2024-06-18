import requests as rq
from bs4 import BeautifulSoup
from bs4.element import Tag
import polars as pl
import random
import time
from typing import Literal
from .utils import _get_game_date, _get_team_abreviation


def random_timesleep(min_seconds: int = 10, max_seconds: int = 30):
    """Pause for a random duration between min_seconds and max_seconds."""
    sleep_duration = random.uniform(min_seconds, max_seconds)
    time.sleep(sleep_duration)


def _get_game_boxscores_url(game: dict, error404: bool = False) -> str:
    return f"https://www.basketball-reference.com/boxscores/{_get_game_date(game, error404)}0{_get_team_abreviation(game.get("home_team_id"))}.html"


def _get_random_user_agent() -> dict[str]:
    """
    Renvoie un User-Agent aléatoire.

    ## Returns:
        str : Un User-Agent choisi aléatoirement

    ## Example(s)
        >>> get_random_user_agent()
        ... "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0"
    """
    valid_user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_2) AppleWebKit/601.3.9 (KHTML, like Gecko) Version/9.0.2 Safari/601.3.9",
        "Mozilla/5.0 (X11; CrOS x86_64 8172.45.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.64 Safari/537.36",
    ]
    user_agent = random.choice(valid_user_agents)
    return {"User-Agent": user_agent}


def _create_session() -> rq.Session:
    session = rq.Session()
    session.headers.update(_get_random_user_agent())
    return session


def _get_team_stat_table_id(
    team_abreviation: str, stats_type: Literal["basic", "advanced"] = "basic"
) -> str:
    if stats_type == "basic":
        return f"box-{team_abreviation}-game-basic"
    else:
        return f"box-{team_abreviation}-game-advanced"


class NBAGameScraper:
    def __init__(self, game: dict, session: rq.sessions.Session):
        self.game = game
        self.url = _get_game_boxscores_url(game)
        self.session = session
        self.boxscore_response = self._get_game_boxscore_page()

    def _get_game_outcome(self, home_team=True):
        if home_team:
            if self.game.get("home_team_score") > self.game.get("away_team_score"):
                return 1
            else:
                return 0
        else:
            if self.game.get("home_team_score") > self.game.get("away_team_score"):
                return 0
            else:
                return 1

    def _get_table_data(self, table: Tag) -> pl.DataFrame:
        headers = [th.getText() for th in table.find_all("tr")[1].find_all("th")]

        # Extract rows data
        rows = table.find_all("tr")[2:]  # Beginning from 3rd row to avoid headers

        data = []

        # To every cell from all rows
        for row in rows:
            cells = row.find_all(["th", "td"])
            cell_data = [cell.getText() for cell in cells]
            data.append(cell_data)

        max_columns = max(len(row) for row in data)

        # Fill empty cells with None
        for row in data:
            while len(row) < max_columns:
                row.append(None)  # ou np.nan si tu préfères

        # Create the complete DataFrame
        df = pl.DataFrame(data, schema=headers[:max_columns])
        return df

    def _get_team_boxscore(self, df: pl.DataFrame, home_team=True) -> pl.DataFrame:
        if home_team:
            team1 = "home_team_id"
            team2 = "away_team_id"
        else:
            team1 = "away_team_id"
            team2 = "home_team_id"
        outcome = self._get_game_outcome(home_team)
        return (
            df.with_columns(pl.lit(0).alias("starter"))
            .with_columns(
                pl.when(
                    pl.arange(0, df.height) < 5
                )  # code to create the starter variable
                .then(1)
                .otherwise(pl.col("starter"))
                .alias("starter")
            )
            .filter(pl.arange(0, df.height) != 5)
            .select(
                pl.lit(self.game.get("id")).alias("game_id"),  # code to add game_id
                pl.lit(_get_team_abreviation(self.game.get(team1))).alias(
                    "team"
                ),  # add team column
                pl.lit(_get_team_abreviation(self.game.get(team2))).alias(
                    "opponent"
                ),  # add opponent column
                pl.lit(outcome).alias("outcome"),  # add game outcome column
                pl.all(),
            )
            .slice(0, df.height - 2)
        )

    def _get_game_boxscore_page(self) -> rq.models.Response:
        response = self.session.get(
            url=self.url,
        )
        if response.status_code == 200:
            print(
                f"Game data extracted successfully: {_get_team_abreviation(self.game.get('home_team_id'))} v {_get_team_abreviation(self.game.get('away_team_id'))}"
            )
        elif response.status_code == 404:
            random_timesleep(5, 15)
            game_url = _get_game_boxscores_url(self.game, error404=True)
            print(
                f"Status {response.status_code}, page doesn't exist, now trying with another link: {game_url}"
            )
            response = self.session.get(
                url=_get_game_boxscores_url(self.game, error404=True),
            )
            if response.status_code == 404:
                print(
                    f"Status {response.status_code}, data could not be collected: {_get_team_abreviation(self.game.get('home_team_id'))} v {_get_team_abreviation(self.game.get('away_team_id'))}"
                )
            else:
                print(
                    f"Game data extracted successfully: {_get_team_abreviation(self.game.get('home_team_id'))} v {_get_team_abreviation(self.game.get('away_team_id'))}"
                )
        return response

    def _get_players_boxscore(
        self, content: BeautifulSoup, stats_type: Literal["basic", "advanced"] = "basic"
    ) -> pl.DataFrame:
        home_table_id = _get_team_stat_table_id(
            _get_team_abreviation(self.game.get("home_team_id")), stats_type
        )
        home_table = content.find(
            "table",
            id=home_table_id,
        )
        df_home = self._get_table_data(home_table)
        df_home = self._get_team_boxscore(df_home, home_team=True)

        away_table_id = _get_team_stat_table_id(
            _get_team_abreviation(self.game.get("away_team_id")), stats_type
        )
        away_table = content.find(
            "table",
            id=away_table_id,
        )
        df_away = self._get_table_data(away_table)
        df_away = self._get_team_boxscore(df_away, home_team=False)

        return df_home.vstack(df_away)

    def _get_team_totals(self, table, home_team=True) -> pl.DataFrame:
        if home_team:
            team1 = "home_team_id"
            team2 = "away_team_id"
            location = 1
        else:
            team1 = "away_team_id"
            team2 = "home_team_id"
            location = 0
        outcome = self._get_game_outcome(home_team)
        return (
            self._get_table_data(table)[-1:]
            .select(
                pl.lit(self.game.get("id")).alias("game_id"),
                pl.lit(_get_team_abreviation(self.game[team1])).alias("team"),
                pl.lit(_get_team_abreviation(self.game[team2])).alias("opponent"),
                pl.lit(outcome).alias("outcome"),
                pl.lit(location).alias("location"),
                pl.all(),
            )
            .drop(["Starters", "+/-"])
        )

    def _get_game_totals(
        self, content: BeautifulSoup, stats_type: Literal["basic", "advanced"] = "basic"
    ) -> pl.DataFrame:
        home_table_id = _get_team_stat_table_id(
            _get_team_abreviation(self.game.get("home_team_id")), stats_type
        )
        home_table = content.find(
            "table",
            id=home_table_id,
        )

        away_table_id = _get_team_stat_table_id(
            _get_team_abreviation(self.game.get("away_team_id")), stats_type
        )
        away_table = content.find(
            "table",
            id=away_table_id,
        )
        return self._get_team_totals(home_table, home_team=True).vstack(
            self._get_team_totals(away_table, home_team=False)
        )

    def fetch_game_data(
        self, stats_type: Literal["basic", "advanced"] = "basic"
    ) -> pl.DataFrame:
        content = BeautifulSoup(self.boxscore_response.content, "html.parser")
        return self._get_players_boxscore(content, stats_type)

    def fetch_total_game_data(
        self, stats_type: Literal["basic", "advanced"] = "basic"
    ) -> pl.DataFrame:
        content = BeautifulSoup(self.boxscore_response.content, "html.parser")
        return self._get_game_totals(content, stats_type)

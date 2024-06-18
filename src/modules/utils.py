import polars as pl
from datetime import date, timedelta
from src.modules.teams import TEAM_ABBREVIATION


def _get_team_abreviation(team_id: int) -> str:
    return TEAM_ABBREVIATION[team_id - 1]


def _get_game_date(game: dict, error404: bool = False) -> str:
    game_date = game["date"]
    # game_date = datetime.fromisoformat(game_date)
    if (
        game_date >= date(2000, 6, 20) and not error404
    ):  # after that season we have to substract one day to get the right url
        game_date -= timedelta(days=1)

    # Return the date following the YYYYMMDD format
    return game_date.strftime("%Y%m%d")


def insert_data_to_database(
    data: pl.DataFrame,
    table: str,
    uri: str,
    **kwargs,
) -> None:
    data.write_database(
        table,
        connection=uri,
        engine="adbc",
        **kwargs,
    )
    print("Data was succefully inserted into the postgresql database.")


def get_postgres_uri(user: str, password: str, server: str, port: str, database: str):
    return f"postgresql://{user}:{password}@{server}:{port}/{database}"

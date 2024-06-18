from basketball_reference_web_scraper import client
from basketball_reference_web_scraper.data import (
    OutputType,
)
import time
from enum import Enum
import polars as pl
from polars.exceptions import DuplicateError
from src.modules.datacleaner import _insert_team_id
from src.modules.utils import insert_data_to_database


def get_season(year: int) -> str:
    return f"{year-1}-{year}"


def export_season_schedule(years: Enum) -> None:
    for year in years:
        client.season_schedule(
            year,
            output_type=OutputType.JSON,
            output_file_path=f"json/schedule_{get_season(year)}.json",
        )
        time.sleep(30)


def create_schedule_df(years: Enum) -> pl.DataFrame:
    for year in years:
        if year == years[0]:
            df_schedule = pl.read_json(f"json/schedule_{get_season(year)}.json")
        else:
            df_schedule = pl.concat(
                [df_schedule, pl.read_json(f"json/schedule_{get_season(year)}.json")]
            )
    return df_schedule


def get_clean_schedule(
    teams: pl.DataFrame, years: Enum = range(1985, 2025, 1)
) -> pl.DataFrame:
    schedule = create_schedule_df(years)
    return (
        schedule.drop_nulls()
        .pipe(add_game_id)
        .with_columns(pl.col("start_time").str.to_datetime())
        .pipe(_insert_team_id, teams, left_column="home_team", right_column="name")
        .pipe(_insert_team_id, teams, left_column="away_team", right_column="name")
        .select(
            [
                "game_id",
                "home_team_id",
                "home_team_score",
                "away_team_id",
                "away_team_score",
                "start_time",
            ]
        )
        .cast(
            {
                "game_id": pl.Int32,
                "home_team_score": pl.Int32,
                "away_team_score": pl.Int32,
                "start_time": pl.Date,
            }
        )
        .rename({"game_id": "id", "start_time": "date"})
    )


def _update_game_id(data: pl.DataFrame) -> pl.DataFrame:
    return data.drop("game_id").with_row_index(name="game_id", offset=1)


def add_game_id(data: pl.DataFrame) -> pl.DataFrame:
    try:
        data = data.with_row_index(name="game_id", offset=1)
    except DuplicateError:
        data = _update_game_id(data)
    return data


def update_game_schedule(
    teams: pl.DataFrame, years_to_update: Enum = [2024]
) -> pl.DataFrame:
    export_season_schedule(years_to_update)
    return get_clean_schedule(teams)


def update_games_table(teams: pl.DataFrame, years_to_update: Enum, uri: str) -> None:
    current_data = pl.read_database_uri(
        query="SELECT * FROM games",
        uri=uri,
    )
    new_data = update_game_schedule(teams, years_to_update)
    new_data = new_data.join(current_data, on="id", how="anti")
    insert_data_to_database(new_data, table="games", uri=uri, if_table_exists="append")

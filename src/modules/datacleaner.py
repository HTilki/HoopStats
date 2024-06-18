import polars as pl


def format_basic_columns(data: pl.DataFrame) -> pl.DataFrame:
    return data.with_columns(
        pl.when(pl.col("MP").is_in(["Did Not Play", "Not With Team"]))
        .then(pl.lit("00:00"))
        .otherwise(pl.col("MP"))
        .name.keep(),
        pl.when(pl.col("FG%") == "").then(None).otherwise(pl.col("FG%")).name.keep(),
        pl.when(pl.col("3P%") == "").then(None).otherwise(pl.col("3P%")).name.keep(),
        pl.when(pl.col("FT%") == "").then(None).otherwise(pl.col("FT%")).name.keep(),
        pl.col("+/-").str.replace(r"^\+", ""),
    )


def cast_basic_columns(data: pl.DataFrame) -> pl.DataFrame:
    return data.with_columns(
        # pl.col("MP") to convert in second (format min:sec into seconds only)
        pl.col("FG").cast(pl.Int32),
        pl.col("FGA").cast(pl.Int32),
        pl.col("FG%").cast(pl.Float32),
        pl.col("3P").cast(pl.Int32),
        pl.col("3PA").cast(pl.Int32),
        pl.col("3P%").cast(pl.Float32),
        pl.col("FT").cast(pl.Int32),
        pl.col("FTA").cast(pl.Int32),
        pl.col("FT%").cast(pl.Float32),
        pl.col("ORB").cast(pl.Int32),
        pl.col("DRB").cast(pl.Int32),
        pl.col("TRB").cast(pl.Int32),
        pl.col("AST").cast(pl.Int32),
        pl.col("STL").cast(pl.Int32),
        pl.col("BLK").cast(pl.Int32),
        pl.col("TOV").cast(pl.Int32),
        pl.col("PF").cast(pl.Int32),
        pl.col("PTS").cast(pl.Int32),
        pl.col("+/-").cast(pl.Int32),
    )


def format_mp_into_sp(df: pl.DataFrame) -> pl.DataFrame:
    def _format_minutes_played_into_seconds_played(x: pl.Series) -> int:
        return int(x[0]) * 60 + int(x[1])

    return df.with_columns(
        (
            pl.col("MP")
            .str.split(":")
            .map_elements(
                lambda x: _format_minutes_played_into_seconds_played(x),
                return_dtype=pl.Int32,
            )
        )
    ).rename({"MP": "SP"})


def _insert_team_id(
    data: pl.DataFrame,
    teams: pl.DataFrame,
    left_column: str,
    right_column: str,
    new_column_name: str = None,
) -> pl.DataFrame:
    if not new_column_name:
        new_column_name = f"{left_column}_id"
    return (
        data.join(teams, how="left", left_on=left_column, right_on=right_column)
        .drop([left_column, "abbreviation", "name", "active"])
        .rename({"id": new_column_name})
    )


def basic_columns_name_change(data: pl.DataFrame) -> pl.DataFrame:
    return data.rename(
        {
            "Starters": "player_id",
            "SP": "seconds_played",
            "FG": "made_field_goal",
            "FGA": "attempted_field_goal",
            "FG%": "field_goal_percent",
            "3P": "made_three_point",
            "3PA": "attempted_three_point",
            "3P%": "three_point_percent",
            "FT": "made_free_throw",
            "FTA": "attempted_free_throw",
            "FT%": "free_throw_percent",
            "ORB": "offensive_rebounds",
            "DRB": "defensive_rebounds",
            "TRB": "total_rebounds",
            "AST": "assists",
            "STL": "steals",
            "BLK": "blocks",
            "TOV": "turnovers",
            "PF": "personal_fouls",
            "PTS": "points",
            "+/-": "plus_minus",
        }
    )


def boxscore_cleaner(data: pl.DataFrame, teams: pl.DataFrame) -> pl.DataFrame:
    return (
        data.pipe(format_basic_columns)
        .pipe(cast_basic_columns)
        .pipe(format_mp_into_sp)
        .pipe(_insert_team_id, teams, left_column="team", right_column="abbreviation")
        .pipe(
            _insert_team_id, teams, left_column="opponent", right_column="abbreviation"
        )
        .pipe(basic_columns_name_change)
    )


def _insert_player_id(): ...  # TODO Scrape all players from 1985 to today and then create player database with ID

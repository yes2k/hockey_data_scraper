import polars as pl
import requests
import datetime
import logging
import os
import mysql.connector
import json

from json_pbp_parser import NHLJsonPbpParser
from html_pbp_parser import NHLHtmlPbpParser
from json_shift_parser import NHLJsonShiftParser
from db_connector import DBConnector


class NHLDataParser():
    json_pbp_parser: NHLJsonPbpParser
    html_pbp_parser: NHLHtmlPbpParser
    json_shift_parser: NHLJsonShiftParser
    db: DBConnector

    def __init__(self, logout_file: str):

        self.json_pbp_parser = NHLJsonPbpParser()
        self.html_pbp_parser = NHLHtmlPbpParser()
        self.json_shift_parser = NHLJsonShiftParser()

        # self.db = DBConnector('./data/database_creds.json')

        logging.basicConfig(
            filename=logout_file,
            encoding="utf-8",
            filemode="a",
            format="{asctime} - {levelname} - {message}",
            style="{",
            datefmt="%Y-%m-%d %H:%M",
        )

    #  date should be formatted as "yyyy-mm-dd"
    def get_game_ids(self, date: str, only_reg_season: bool) -> dict | None:
        url = "https://api-web.nhle.com/v1/schedule/" + date

        try:
            data = requests.get(url).json()
        except Exception:
            print("url not found")
            return None

        game_id_data = {
            "game_id": [],
            "date": [],
            "home_team": [],
            "away_team": []
        }
        d = data["gameWeek"][0]
        for g in d["games"]:
            if only_reg_season:
                if g["gameType"] == 2:
                    game_id_data["game_id"].append(g["id"])
                    game_id_data["date"].append(d["date"])
                    game_id_data["home_team"].append(g["awayTeam"]["abbrev"])
                    game_id_data["away_team"].append(g["homeTeam"]["abbrev"])
            else:
                if g["gameType"] in [2, 3]:
                    game_id_data["game_id"].append(g["id"])
                    game_id_data["date"].append(d["date"])
                    game_id_data["home_team"].append(g["awayTeam"]["abbrev"])
                    game_id_data["away_team"].append(g["homeTeam"]["abbrev"])
        return game_id_data

    def parse_data_to_csv(
            self,
            start_date: str,
            end_date: str,
            only_reg_season: bool,
            out_path: str
    ):
        # checking if path to output csvs exists
        if not os.path.exists(out_path):
            os.makedirs(out_path)

        if not os.path.exists(os.path.join(out_path, "json_pbp_game")):
            os.makedirs(os.path.join(out_path, "json_pbp_game"))
        if not os.path.exists(os.path.join(out_path, "json_pbp_player_info")):
            os.makedirs(os.path.join(out_path, "json_pbp_player_info"))
        if not os.path.exists(os.path.join(out_path, "json_pbp_plays")):
            os.makedirs(os.path.join(out_path, "json_pbp_plays"))
        if not os.path.exists(os.path.join(out_path, "json_shift_games")):
            os.makedirs(os.path.join(out_path, "json_shift_games"))
        if not os.path.exists(os.path.join(out_path, "html_pbp_games")):
            os.makedirs(os.path.join(out_path, "html_pbp_games"))


        parsed_start_date = datetime.date(
            int(start_date[0:4]), int(start_date[5:7]), int(start_date[8:10]))
        parsed_end_date = datetime.date(
            int(end_date[0:4]), int(end_date[5:7]), int(end_date[8:10]))
        date_range = (
            pl.date_range(
                parsed_start_date,
                parsed_end_date, eager=True
            ).cast(pl.String).alias("date")
        ).to_list()

        json_pbp_games = []
        json_shift_games = []
        html_pbp_games = []
        for date in date_range:
            game_ids = self.get_game_ids(date, only_reg_season)
            if game_ids:
                for g in game_ids["game_id"]:
                    print(g)
                    # parsing json pbp
                    try:
                        # json_pbp_games.append(self.json_pbp_parser.parse(g))
                        json_pbp_game = self.json_pbp_parser.parse(g)
                        json_pbp_game.game_info_to_df().write_csv(
                            os.path.join(out_path, "json_pbp_game", str(g)+".csv")
                        )

                        json_pbp_game.players_to_df().write_csv(
                            os.path.join(out_path, "json_pbp_player_info", str(g)+".csv")
                        )

                        json_pbp_game.plays_to_df().write_csv(
                            os.path.join(out_path, "json_pbp_plays", str(g)+".csv")
                        )
                    except Exception:
                        logging.error(
                            f"json pbp parser failed to parse game {g}")

                    # parsing json shift pbp
                    try:
                        json_shift_game = self.json_shift_parser.parse(g)
                        json_shift_game.to_df().write_csv(
                            os.path.join(out_path, "json_shift_games", str(g)+".csv")
                        )
                    except Exception:
                        logging.error(
                            f"json shift parser failed to parse game {g}")

                    # parsing html pbp
                    try:
                        html_pbp_games = self.html_pbp_parser.parse(str(g))
                        html_pbp_games.to_df().write_csv(
                            os.path.join(out_path, "html_pbp_games", str(g)+".csv")
                        )
                    except Exception:
                        logging.error(
                            f"html pbp parser failed to parse game {g}")

        # writing to a csv file for now
        # pl.concat(map(lambda x: x.game_info_to_df(), json_pbp_games), how="diagonal_relaxed").write_csv(
            # os.path.join(out_path, "json_pbp_game_info.csv"))
        # pl.concat(map(lambda x: x.players_to_df(), json_pbp_games), how="diagonal_relaxed").write_csv(
            # os.path.join(out_path, "json_pbp_player_info.csv"))
        # pl.concat(map(lambda x: x.plays_to_df(), json_pbp_games), how="diagonal_relaxed").write_csv(
            # os.path.join(out_path, "json_pbp_plays.csv"))
        # pl.concat(map(lambda x: x.to_df(), json_shift_games), how="diagonal_relaxed").write_csv(
            # os.path.join(out_path, "json_shift_game_info.csv"))
        # pl.concat(map(lambda x: x.to_df(), html_pbp_games), how="diagonal_relaxed").write_csv(
            # os.path.join(out_path, "html_pbp_plays.csv"))

    def build_database(
        self,
        start_date: str,
        end_date: str,
        only_reg_season: bool,
        out_path: str,
        sql_file_path: str
    ):
        if not os.path.exists(out_path):
            self.__parse_data_to_csv(
                start_date, end_date, only_reg_season, out_path)

        self.db.execute_sql_file(sql_file_path)


    def update_database(self, only_reg_season: bool):
        max_date = self.db.get_query_result(
            "SELECT MAX(date) FROM nhl_api_data.json_pbp_game_info"
        )[0,0]

        # Range of dates to update
        date_range = (
            pl
            .date_range(
                max_date + datetime.timedelta(days=1),
                datetime.date.today() - datetime.timedelta(days=1),
                eager=True
            )
            .cast(pl.String)
            .alias("date").to_list()
        )
        print(f"Date range to update: {date_range}")

        for date in date_range:
            game_ids = self.get_game_ids(date, only_reg_season)
            if game_ids:
                for g in game_ids["game_id"]:
                    print(g)
                    # parsing json pbp
                    try:
                        out = self.json_pbp_parser.parse(g)
                    except Exception:
                        logging.error(
                            f"json pbp parser failed to parse game {g}")

                    # writing to database
                    try:
                        out.game_info_to_df()
                    except Exception:
                        logging.error(f"json pbp parser failed to write game {
                                      g} to database")

                    # parsing json shift pbp
                    try:
                        self.json_shift_parser.parse(g)
                    except Exception:
                        logging.error(
                            f"json shift parser failed to parse game {g}")

                    # parsing html pbp
                    try:
                        self.html_pbp_parser.parse(str(g))
                    except Exception:
                        logging.error(
                            f"html pbp parser failed to parse game {g}")


if __name__ == "__main__":
    parser = NHLDataParser("./data/nhl_data_parser.log")
    # parser.update_database(True)
    parser.parse_data_to_csv(
        "2018-10-01",
        datetime.date.today().strftime("%Y-%m-%d"),
        True,
        "./data/csvs"
    ) 
    # parser.build_database("2018-10-01", datetime.date.today().strftime("%Y-%m-%d"), True, "./data/csvs", "./data/src/create_and_load_tables.sql")

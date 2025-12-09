import argparse
import datetime
import logging
import os
import shutil

import polars as pl
import requests

from db_connector import DBConnector
from sub_parsers.html_pbp_parser import NHLHtmlPbpParser
from sub_parsers.json_pbp_parser import NHLJsonPbpParser
from sub_parsers.json_shift_parser import NHLJsonShiftParser


class NHLDataParser:
    json_pbp_parser: NHLJsonPbpParser
    html_pbp_parser: NHLHtmlPbpParser
    json_shift_parser: NHLJsonShiftParser
    db: DBConnector

    def __init__(self, logout_file: str, db_cred_path: str):

        self.json_pbp_parser = NHLJsonPbpParser()
        self.html_pbp_parser = NHLHtmlPbpParser()
        self.json_shift_parser = NHLJsonShiftParser()

        self.db = DBConnector(db_cred_path)

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

        game_id_data = {"game_id": [], "date": [], "home_team": [], "away_team": []}
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

    # scraping nhl api data and saving it to csvs
    def parse_data_to_csvs(
        self,
        start_date: str,
        end_date: str,
        only_reg_season: bool,
        backup_out_path: str,
    ) -> None:
        # getting the date range to parse
        parsed_start_date = datetime.date(
            int(start_date[0:4]), int(start_date[5:7]), int(start_date[8:10])
        )
        parsed_end_date = datetime.date(
            int(end_date[0:4]), int(end_date[5:7]), int(end_date[8:10])
        )
        date_range = (
            pl.date_range(parsed_start_date, parsed_end_date, eager=True)
            .cast(pl.String)
            .alias("date")
        ).to_list()

        # function to check if folder exists and if it does, empty it
        def folder_check(path: str):
            if os.path.exists(path):
                for filename in os.listdir(path):
                    file_path = os.path.join(path, filename)
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
            else:
                os.mkdir(path)

        out_paths = {
            "html_pbp_plays": os.path.join("./html_pbp_plays"),
            "json_pbp_game_info": os.path.join("./json_pbp_game_info"),
            "json_pbp_player_info": os.path.join("./json_pbp_player_info"),
            "json_pbp_plays": os.path.join("./json_pbp_plays"),
            "json_shift_info": os.path.join("./json_shift_info"),
        }

        for _, v in out_paths.items():
            folder_check(v)

        # scraping data from nhl api and saving them into csvs
        for date in date_range:
            game_ids = self.get_game_ids(date, only_reg_season)
            if game_ids:
                for g in game_ids["game_id"]:
                    print(g)
                    # parsing json pbp
                    try:
                        json_pbp_game = self.json_pbp_parser.parse(g)
                        json_pbp_game.game_info_to_df().write_csv(
                            os.path.join(
                                out_paths["json_pbp_game_info"], str(g) + ".csv"
                            )
                        )

                        json_pbp_game.players_to_df().write_csv(
                            os.path.join(
                                out_paths["json_pbp_player_info"], str(g) + ".csv"
                            )
                        )

                        json_pbp_game.plays_to_df().write_csv(
                            os.path.join(out_paths["json_pbp_plays"], str(g) + ".csv")
                        )
                    except Exception:
                        logging.error(f"json pbp parser failed to parse game {g}")

                    # parsing json shift pbp
                    try:
                        json_shift_game = self.json_shift_parser.parse(g)
                        json_shift_game.to_df().write_csv(
                            os.path.join(out_paths["json_shift_info"], str(g) + ".csv")
                        )
                    except Exception:
                        logging.error(f"json shift parser failed to parse game {g}")

                    # parsing html pbp
                    try:
                        html_pbp_games = self.html_pbp_parser.parse(str(g))
                        html_pbp_games.to_df().write_csv(
                            os.path.join(out_paths["html_pbp_plays"], str(g) + ".csv")
                        )
                    except Exception:
                        logging.error(f"html pbp parser failed to parse game {g}")

        if not os.path.exists(backup_out_path):
            os.makedirs(backup_out_path)

        for k, v in out_paths.items():
            csv_files = [
                os.path.join(v, f) for f in os.listdir(v) if f.endswith(".csv")
            ]
            df = pl.concat([pl.read_csv(f) for f in csv_files], how="diagonal_relaxed")
            df.write_csv(os.path.join(backup_out_path, k + ".csv"))
            shutil.rmtree(v)

    # create tables and load data from csvs files
    def build_db_from_csvs(
        self,
        sql_file_path: str,
    ) -> None:
        # create databases and tables and loads csv into db
        self.db.execute_sql_file(sql_file_path)

    def build_db_from_scratch(
        self,
        start_date: str,
        end_date: str,
        only_reg_season: bool,
        backup_out_path: str,
        sql_file_path: str,
    ) -> None:
        self.parse_data_to_csvs(start_date, end_date, only_reg_season, backup_out_path)

        self.build_db_from_csvs(sql_file_path)

    def test(self):
        cursor = self.db.mydb.cursor()
        cursor.execute("USE nhl_api_data;")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS test_table (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        cursor.execute("""
            INSERT INTO test_table (name, email) VALUES
            ('Alice Smith', 'alice@example.com'),
            ('Bob Johnson', 'bob@example.com'),
            ('Charlie Lee', 'charlie@example.com'),
            ('Dana White', NULL);
        """)
        self.db.mydb.commit()
        cursor.close()

    def update_database(self, only_reg_season: bool):
        # get max date in database, get current date, iterate over all dates in between
        # get gameid's for each date, parse them and update them into the database

        max_date = self.db.get_query_result(
            "SELECT MAX(date) FROM nhl_api_data.json_pbp_game_info"
        )[0, 0]

        # Range of dates to update
        date_range = (
            pl.date_range(
                max_date + datetime.timedelta(days=1),
                datetime.date.today() - datetime.timedelta(days=1),
                eager=True,
            )
            .cast(pl.String)
            .alias("date")
            .to_list()
        )
        print(f"Date range to update: {','.join(date_range)}")

        for date in date_range:
            game_ids = self.get_game_ids(date, only_reg_season)
            if game_ids:
                for g in game_ids["game_id"]:
                    print(g)
                    # parsing json pbp
                    try:
                        out = self.json_pbp_parser.parse(g)
                    except Exception:
                        logging.error(f"json pbp parser failed to parse game {g}")

                    # writing to database
                    try:
                        self.db.push_dataframe_to_db(
                            out.game_info_to_df(), "nhl_api_data.json_pbp_game_info"
                        )
                        self.db.push_dataframe_to_db(
                            out.players_to_df(), "nhl_api_data.json_pbp_player_info"
                        )
                        self.db.push_dataframe_to_db(
                            out.plays_to_df(), "nhl_api_data.json_pbp_plays"
                        )
                    except Exception:
                        logging.error(
                            f"json pbp parser failed to write game {g} to database"
                        )

                    # parsing json shift pbp
                    try:
                        out2 = self.json_shift_parser.parse(g)
                    except Exception:
                        logging.error(f"json shift parser failed to parse game {g}")
                    try:
                        self.db.push_dataframe_to_db(
                            out2.to_df(), "nhl_api_data.json_shift_info"
                        )
                    except Exception:
                        logging.error(
                            f"json shift table failed to write {g} to database"
                        )

                    # parsing html pbp
                    try:
                        out3 = self.html_pbp_parser.parse(str(g))
                    except Exception:
                        logging.error(f"html pbp parser failed to parse game {g}")

                    try:
                        self.db.push_dataframe_to_db(
                            out3.to_df(), "nhl_api_data.html_pbp_plays"
                        )
                    except Exception:
                        logging.error(f"html pbp parser failed write to db {g}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NHL Data Parser CLI")
    parser.add_argument("--logfile", type=str, default="./src/nhl_data_parser.log", help="Path to log file")
    parser.add_argument("--db_cred_path", type=str, default="./database_creds.json", help="Path to database credential json file")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Subparser for creating csv backup
    parquet_parser = subparsers.add_parser(
        "create_csv_backup", help="Scrape and save data to csv backup"
    )
    parquet_parser.add_argument(
        "--start_date", type=str, required=True, help="Start date (YYYY-MM-DD)"
    )
    parquet_parser.add_argument(
        "--end_date", type=str, required=True, help="End date (YYYY-MM-DD)"
    )
    parquet_parser.add_argument(
        "--only_reg_season", action="store_true", help="Only regular season games"
    )
    parquet_parser.add_argument(
        "--backup_out_path", type=str, required=True, help="Output path for csv backup"
    )

    # Subparser for building db from scratch
    scratch_parser = subparsers.add_parser(
        "build_from_scratch", help="Scrape, save to csv, and build DB"
    )
    scratch_parser.add_argument(
        "--start_date", type=str, required=True, help="Start date (YYYY-MM-DD)"
    )
    scratch_parser.add_argument(
        "--end_date", type=str, required=True, help="End date (YYYY-MM-DD)"
    )
    scratch_parser.add_argument(
        "--only_reg_season", action="store_true", help="Only regular season games"
    )
    scratch_parser.add_argument(
        "--backup_out_path", type=str, required=True, help="Output path for csv backup"
    )
    scratch_parser.add_argument(
        "--sql_file_path", type=str, required=True, help="SQL file path for DB schema"
    )

    # Subparser for building db from csv backup
    csv_db_parser = subparsers.add_parser(
        "build_from_csv_backup", help="Build DB from existing csv backup"
    )
    csv_db_parser.add_argument(
        "--csv_path", type=str, required=True, help="Path to csv backup"
    )
    csv_db_parser.add_argument(
        "--sql_file_path", type=str, required=True, help="SQL file path for DB schema"
    )

    # Subparser for updating the database
    update_db_parser = subparsers.add_parser(
        "update_database", help="Update the database with new games"
    )
    update_db_parser.add_argument(
        "--only_reg_season", action="store_true", help="Only regular season games"
    )

    args = parser.parse_args()
    nhl_parser = NHLDataParser(args.logfile, args.db_cred_path)

    if args.command == "create_csv_backup":
        nhl_parser.parse_data_to_csvs(
            args.start_date, args.end_date, args.only_reg_season, args.backup_out_path
        )
    elif args.command == "build_from_scratch":
        nhl_parser.build_db_from_scratch(
            args.start_date,
            args.end_date,
            args.only_reg_season,
            args.backup_out_path,
            args.sql_file_path,
        )
    elif args.command == "build_from_csv_backup":
        nhl_parser.build_db_from_csvs(args.sql_file_path)
    elif args.command == "update_database":
        nhl_parser.update_database(args.only_reg_season)

import polars as pl
import requests
import datetime
import logging
import os
import shutil

from sub_parsers.json_pbp_parser import NHLJsonPbpParser
from sub_parsers.html_pbp_parser import NHLHtmlPbpParser
from sub_parsers.json_shift_parser import NHLJsonShiftParser
from db_connector import DBConnector
import shutil


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


    # scraping nhl api data and saving it to a set of parquet data
    def parse_data_to_parquet(
            self,
            start_date: str,
            end_date: str,
            only_reg_season: bool,
            out_path: str,
            backup_out_path: str
    ) -> None:
        # getting the date range to parse
        parsed_start_date = datetime.date(
            int(start_date[0:4]),
            int(start_date[5:7]),
            int(start_date[8:10])
        )
        parsed_end_date = datetime.date(
            int(end_date[0:4]),
            int(end_date[5:7]),
            int(end_date[8:10])
        )
        date_range = (
            pl.date_range(
                parsed_start_date,
                parsed_end_date, eager=True
            ).cast(pl.String).alias("date")
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
            "html_pbp_plays": os.path.join(out_path, "html_pbp_plays"),
            "json_pbp_game_info": os.path.join(out_path, "json_pbp_game_info"),
            "json_pbp_player_info": os.path.join(out_path, "json_pbp_player_info"),
            "json_pbp_plays": os.path.join(out_path, "json_pbp_plays"),
            "json_shift_info": os.path.join(out_path, "json_shift_info"),
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
                            os.path.join(out_paths["json_pbp_game_info"], str(g)+".csv")
                        )

                        json_pbp_game.players_to_df().write_csv(
                            os.path.join(out_paths["json_pbp_player_info"], str(g)+".csv")
                        )

                        json_pbp_game.plays_to_df().write_csv(
                            os.path.join(out_paths["json_pbp_plays"], str(g)+".csv")
                        )
                    except Exception:
                        logging.error(
                            f"json pbp parser failed to parse game {g}")

                    # parsing json shift pbp
                    try:
                        json_shift_game = self.json_shift_parser.parse(g)
                        json_shift_game.to_df().write_csv(
                            os.path.join(out_paths["json_shift_info"], str(g)+".csv")
                        )
                    except Exception:
                        logging.error(
                            f"json shift parser failed to parse game {g}")

                    # parsing html pbp
                    try:
                        html_pbp_games = self.html_pbp_parser.parse(str(g))
                        html_pbp_games.to_df().write_csv(
                            os.path.join(out_paths["html_pbp_plays"], str(g)+".csv")
                        )
                    except Exception:
                        logging.error(
                            f"html pbp parser failed to parse game {g}")

        for k, v in out_paths.items():
            csv_files = [os.path.join(v, f) for f in os.listdir(v) if f.endswith('.csv')]
            df = pl.concat([pl.read_csv(f) for f in csv_files])
            df.write_parquet(os.path.join(backup_out_path, k+".parquet"))
            shutil.rmtree(v)
    

    # create tables and load data from parquet files
    def build_db_from_parquet_backup(
        self,
        parquet_path: str,
        sql_file_path: str,
    ) -> None:
        # create databases and tables 
        self.db.execute_sql_file(sql_file_path)
        
        # Load data from parquet files and 
        # put them in the db
        for file in os.listdir(parquet_path):
            if file.endswith(".parquet"):
                table_name, _ = os.path.splitext(file)[0]
                parquet_file = os.path.join(parquet_path, file)
                self.db.load_parquet_to_mysql(parquet_file, table_name)


    def build_db_from_scratch(
        self,
        start_date: str,
        end_date: str,
        only_reg_season: bool,
        out_path: str,
        backup_out_path: str,
        sql_file_path: str,
    ) -> None:
        self.parse_data_to_parquet(
            start_date,
            end_date,
            only_reg_season,
            out_path,
            backup_out_path
        )

        self.build_db_from_parquet_backup(
            backup_out_path,
            sql_file_path
        )
    


    def update_database(self, only_reg_season: bool):
        # get max date in database, get current date, iterate over all dates in between
        # get gameid's for each date, parse them and update them into the database

       
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
        print(f"Date range to update: {",".join(date_range)}")

        for date in date_range:
            game_ids = self.get_game_ids(date, only_reg_season)
            if game_ids:
                for g in game_ids["game_id"]:
                    # parsing json pbp
                    try:
                        out = self.json_pbp_parser.parse(g)
                    except Exception:
                        logging.error(f"json pbp parser failed to parse game {g}")

                    # writing to database
                    try:
                        self.db.push_dataframe_to_db(out.game_info_to_df(), "json_pbp_game_info")
                        self.db.push_dataframe_to_db(out.players_to_df(), "json_pbp_player_info")
                        self.db.push_dataframe_to_db(out.plays_to_df(), "json_pbp_plays")
                    except Exception:
                        logging.error(f"json pbp parser failed to write game {g} to database")



                    # parsing json shift pbp
                    try:
                        out2 = self.json_shift_parser.parse(g)
                    except Exception:
                        logging.error(f"json shift parser failed to parse game {g}")
                    try:
                        self.db.push_dataframe_to_db(out2.to_df(), "json_shift_info")
                    except Exception:
                        logging.error(f"json shift table failed to write {g} to database")



                    # parsing html pbp
                    try:
                        out3 = self.html_pbp_parser.parse(str(g))
                    except Exception:
                        logging.error(f"html pbp parser failed to parse game {g}")

                    try:
                        self.db.push_dataframe_to_db(out3.to_df(), "html_pbp_plays")
                    except Exception:
                        logging.error(f"html pbp parser failed write to db {g}")


# if __name__ == "__main__":
    # parser = NHLDataParser("./data/src/nhl_data_parser.log")
    # parser.update_database(True)

    # parser.build_database("2018-10-01", datetime.date.today().strftime("%Y-%m-%d"), True, "./data/csvs", "./data/src/create_and_load_tables.sql")

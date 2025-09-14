from dataclasses import dataclass
import requests
import polars as pl


@dataclass
class Shift:
    id: int
    start_time: str
    end_time: str
    period: int
    duration: str
    first_name: str
    last_name: str
    player_id: int
    team_id: int
    team_abbrev: str

    def to_dict(self):
        return {
            "id": self.id,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "period": self.period,
            "duration": self.duration,
            "first_name": self.first_name,
            "last_name": self.last_name,
            "player_id": self.player_id,
            "team_id": self.team_id,
            "team_abbrev": self.team_abbrev
        }


@dataclass
class ShiftInfo:
    game_id: str
    shift_list: list[Shift]

    def to_df(self) -> pl.DataFrame:
        out = {
            "game_id": [],
            "id": [],
            "start_time": [],
            "end_time": [],
            "period": [],
            "duration": [],
            "first_name": [],
            "last_name": [],
            "player_id": [],
            "team_id": [],
            "team_abbrev": []
        }

        for shift in self.shift_list:
            out["game_id"].append(self.game_id)
            out["id"].append(shift.id)
            out["start_time"].append(shift.start_time)
            out["end_time"].append(shift.end_time)
            out["period"].append(shift.period)
            out["duration"].append(shift.duration)
            out["first_name"].append(shift.first_name)
            out["last_name"].append(shift.last_name)
            out["player_id"].append(shift.player_id)
            out["team_id"].append(shift.team_id)
            out["team_abbrev"].append(shift.team_abbrev)
        
        return pl.DataFrame(out)



class NHLJsonShiftParser():

    def __init__(self) -> None:
        pass

    def parse(self, game_id: str) -> ShiftInfo:
        url = "https://api.nhle.com/stats/rest/en/shiftcharts?cayenneExp=gameId=" + str(game_id)

        try:
            json_res = requests.get(url).json()
        except:
            raise(RuntimeError(f"shift chart for game_id: {game_id} not found"))

        shift_info = ShiftInfo(game_id, [])
        for shift in json_res["data"]:
            shift_info.shift_list.append(
                Shift(
                    shift["id"],
                    shift["startTime"],
                    shift["endTime"],
                    shift["period"],
                    shift["duration"],
                    shift["firstName"],
                    shift["lastName"],
                    shift["playerId"],
                    shift["teamId"],
                    shift["teamAbbrev"]
                )
            )

        return shift_info

    def to_df(self, shifts: list[ShiftInfo]) -> dict[str, pl.DataFrame]:
        return {
            "shift_info": pl.concat(map(lambda x: x.to_df(), shifts), how = "diagonal_relaxed")
        }
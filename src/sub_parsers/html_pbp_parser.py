import polars as pl
from sub_parsers.json_pbp_parser import EventType, event_type_to_string
from dataclasses import dataclass
from bs4 import BeautifulSoup
import requests
import re

@dataclass
class PbpHtmlPlay:
    game_id: str
    n: str
    period: str
    strength: str
    time_elapsed: str
    event: EventType | None
    description: str
    away_on_ice_player_sweater_num: list[str]
    home_on_ice_player_sweater_num: list[str]

@dataclass
class PbpHtml:
    list_of_plays: list[PbpHtmlPlay]

    def to_df(self) -> pl.DataFrame:
        df = {
            "game_id": [],
            "n": [],
            "period": [],
            "strength": [],
            "time_elapsed": [],
            "event": [],
            "description": [],
            "away_on_ice_p1": [],
            "away_on_ice_p2": [],
            "away_on_ice_p3": [],
            "away_on_ice_p4": [],
            "away_on_ice_p5": [],
            "away_on_ice_p6": [],
            "away_on_ice_p7": [],
            "away_on_ice_p8": [],
            "away_on_ice_p9": [],
            "home_on_ice_p1": [],
            "home_on_ice_p2": [],
            "home_on_ice_p3": [],
            "home_on_ice_p4": [],
            "home_on_ice_p5": [],
            "home_on_ice_p6": [],
            "home_on_ice_p7": [],
            "home_on_ice_p8": [],
            "home_on_ice_p9": []
        }

        for play in self.list_of_plays:
            df["game_id"].append(play.game_id)
            df["n"].append(play.n)
            df["period"].append(play.period)
            df["strength"].append(play.strength)
            df["time_elapsed"].append(play.time_elapsed)
            df["event"].append(event_type_to_string(play.event))
            df["description"].append(play.description)
            for i in range(1, 10):
                if i < len(play.away_on_ice_player_sweater_num):
                    df["away_on_ice_p" + str(i)].append(play.away_on_ice_player_sweater_num[i])
                else:
                    df["away_on_ice_p" + str(i)].append(None)

            for i in range(1, 10):
                if i < len(play.home_on_ice_player_sweater_num):
                    df["home_on_ice_p" + str(i)].append(play.home_on_ice_player_sweater_num[i])
                else:
                    df["home_on_ice_p" + str(i)].append(None)
        
        return ((
            pl.from_dict(df).fill_null("")
        )) 


class NHLHtmlPbpParser:

    def __init__(self) -> None:
        pass

    def parse(self, game_id: str) -> PbpHtml:
        season = game_id[0:4] + str(int(game_id[0:4]) + 1)
        url = "https://www.nhl.com/scores/htmlreports/"+ season + "/PL" + game_id[4:] + ".HTM"

        data = requests.get(url).text
        soup = BeautifulSoup(data, 'html.parser')

        out = PbpHtml([])

        for page in soup.find_all(attrs={"class": "tablewidth"}):
            for tr in page.find_all("tr", class_ = re.compile("(even|odd)Color")): # type: ignore
                td = tr.find_all("td", recursive=False) # type: ignore

                away_on_ice_player_sweater_num = []
                home_on_ice_player_sweater_num = []
                for i in range(len(td)):
                    if i == 0:
                        n = int(td[i].get_text())
                    elif i == 1:
                        period = td[i].get_text()
                    elif i == 2:
                        strength = td[i].get_text(strip=True)
                    elif i == 3:
                        all_time = re.findall("[0-9]?[0-9]:[0-9][0-9]", str(td[i]))
                        time_elapsed = all_time[0]
                    elif i == 4:
                        event = html_string_to_event_type(td[i].get_text(strip=True))
                    elif i == 5:
                        description = td[i].get_text(strip=True)
                    elif i == 6:
                        x = td[i].get_text()
                        nums_on_ice = re.findall("[0-9]+", x)
                        # print(re.findall("[A-Z]", x))
                        for i in range(1, 10):
                            if i <= len(nums_on_ice):
                                away_on_ice_player_sweater_num.append(nums_on_ice[i-1])
                            else:
                                away_on_ice_player_sweater_num.append("")
                    elif i == 7:
                        x = td[i].get_text()
                        nums_on_ice = re.findall("[0-9]+", x)
                        # print(re.findall("[A-Z]", x))
                        for i in range(1, 10):
                            if i <= len(nums_on_ice):
                                home_on_ice_player_sweater_num.append(nums_on_ice[i-1])
                            else:
                                home_on_ice_player_sweater_num.append("")
                out.list_of_plays.append(PbpHtmlPlay(
                    game_id = game_id,
                    n = str(n),
                    period = period,
                    strength = strength,
                    time_elapsed = time_elapsed,
                    event = event,
                    description = description,
                    away_on_ice_player_sweater_num = away_on_ice_player_sweater_num,
                    home_on_ice_player_sweater_num = home_on_ice_player_sweater_num
                ))
        return out
    


def pbp_html_list_to_df(dat: PbpHtml) -> pl.DataFrame:
    df = {
        "game_id": [],
        "n": [],
        "period": [],
        "strength": [],
        "time_elapsed": [],
        "event": [],
        "description": [],
        "away_on_ice_p1": [],
        "away_on_ice_p2": [],
        "away_on_ice_p3": [],
        "away_on_ice_p4": [],
        "away_on_ice_p5": [],
        "away_on_ice_p6": [],
        "away_on_ice_p7": [],
        "away_on_ice_p8": [],
        "away_on_ice_p9": [],
        "home_on_ice_p1": [],
        "home_on_ice_p2": [],
        "home_on_ice_p3": [],
        "home_on_ice_p4": [],
        "home_on_ice_p5": [],
        "home_on_ice_p6": [],
        "home_on_ice_p7": [],
        "home_on_ice_p8": [],
        "home_on_ice_p9": []
    }

    for play in dat.list_of_plays:
        df["game_id"].append(play.game_id)
        df["n"].append(play.n)
        df["period"].append(play.period)
        df["strength"].append(play.strength)
        df["time_elapsed"].append(play.time_elapsed)
        df["event"].append(event_type_to_string(play.event))
        df["description"].append(play.description)
        for i in range(1, 10):
            if i < len(play.away_on_ice_player_sweater_num):
                df["away_on_ice_p" + str(i)].append(play.away_on_ice_player_sweater_num[i])
            else:
                df["away_on_ice_p" + str(i)].append(None)

        for i in range(1, 10):
            if i < len(play.home_on_ice_player_sweater_num):
                df["home_on_ice_p" + str(i)].append(play.home_on_ice_player_sweater_num[i])
            else:
                df["home_on_ice_p" + str(i)].append(None)
    
    return ((
        pl.from_dict(df).fill_null("")
    ))



def html_string_to_event_type(s: str) -> EventType | None:
    match s:
        case "PGSTR": None
        case "PGEND": None
        case "ANTHEM": None,
        case "PSTR": return(EventType.PeriodStart)
        case "FAC": return(EventType.Faceoff)
        case "SHOT": return(EventType.ShotOnGoal)
        case "STOP": return(EventType.Stoppage)
        case "MISS": return(EventType.MissedShot)
        case "HIT": return(EventType.Hit)
        case "BLOCK": return(EventType.BlockedShot)
        case "GIVE": return(EventType.Giveaway)
        case "TAKE": return(EventType.Takeaway)
        case "GOAL": return(EventType.Goal)
        case "PENL": return(EventType.Penalty)
        case "DELPEN": return(EventType.DelayedPenalty)
        case "PEND": return(EventType.PeriodEnd)
        case "GEND": return(EventType.GameEnd)
        case "SOC": return(EventType.ShootoutComplete)
        case "GOFF": None
        case "EISTR": None
        case "EIEND": None
        case "EGT": None
        case "EGPID": None
        case "CHL": None
        case "PBOX": None
        case "SPC": None
        case _: raise(ValueError(s + "is not a valid EventType"))
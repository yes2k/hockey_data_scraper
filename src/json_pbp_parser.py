import polars as pl
from enum import Enum
from dataclasses import dataclass
import requests
from collections.abc import Iterable

EventType = Enum('EventType', [
    "PeriodStart", "Faceoff", "ShotOnGoal", "Stoppage",
    "MissedShot", "Hit", "BlockedShot", "Giveaway", "Goal",
    "Takeaway", "Penalty", "DelayedPenalty", "PeriodEnd",
    "GameEnd", "ShootoutComplete", "FailedShotAttempt"   
])

ShotType = Enum('ShotType', [
    "Wrist", "Slap", "Backhand", "Snap",
    "TipIn", "Deflected", "WrapAround", "BetweenLegs",
    "Bat", "Poke", "Cradle"
])

ZoneCode = Enum('ZoneCode', [
    "Offensive", "Defensive", 'Neutral'
])

PlayerPosition = Enum('PlayerPosition', [
    'C', 'L', 'R', 'D', 'G' 
])

@dataclass
class Team:
    name: str
    abrv: str
    id: int

@dataclass
class Player:
    team_id: int
    first_name: str
    last_name: str
    id: int
    position: PlayerPosition
    sweater_number: int


@dataclass
class Play:
    n: int
    event_type: EventType | None
    period: int
    period_type: str
    time_in_period: str
    time_remaining: str
    event_owner_team_id: int | None
    p1: int | None
    p2: int | None
    p3: int | None
    goalie: int | None
    shot_type: ShotType | None
    x: int | None
    y: int | None
    reason: str | None
    penalty_duration: int | None
        

@dataclass
class Game:
    game_id: str
    season: str
    date: str
    away_team: Team
    home_team: Team
    away_team_goals: int
    home_team_goals: int
    venue: str
    venue_location: str
    referee_1: str | None
    referee_2: str | None
    linesmen_1: str | None
    linesmen_2: str | None
    home_coach: str | None
    away_coach: str | None
    players: list[Player]
    plays: list[Play]

    def game_info_to_df(self) -> pl.DataFrame:
        return pl.DataFrame({
            "game_id": self.game_id,
            "season": self.season,
            "date": self.date,
            "away_team_name": self.away_team.name,
            "away_team_abrv": self.away_team.abrv,
            "away_team_id": self.away_team.id,
            "away_team_goals": self.away_team_goals,
            "home_team_name": self.home_team.name,
            "home_team_abrv": self.home_team.abrv,
            "home_team_id": self.home_team.id,
            "home_team_goals": self.home_team_goals,
            "venue": self.venue,
            "venue_location": self.venue_location,
            "referee_1": self.referee_1,
            "referee_2": self.referee_2,
            "linesmen_1": self.linesmen_1,
            "linesmen_2": self.linesmen_2,
            "home_coach": self.home_coach,
            "away_coach": self.away_coach
        })

    def players_to_df(self) -> pl.DataFrame:
        df = {
            "team_id": [],
            "first_name": [],
            "last_name": [],
            "id": [],
            "position": [],
            "sweater_number": []
        }

        for player in self.players:
            df["team_id"].append(player.team_id)
            df["first_name"].append(player.first_name)
            df["last_name"].append(player.last_name)
            df["id"].append(player.id)
            df["position"].append(player_position_to_string(player.position))
            df["sweater_number"].append(player.sweater_number)

        return pl.DataFrame(df).with_columns(pl.lit(self.game_id).alias("game_id"))

    def plays_to_df(self) -> pl.DataFrame:
        df = {
            "n": [],
            "event_type": [],
            "period": [],
            "period_type": [],
            "time_in_period": [],
            "time_remaining": [],
            "event_owner_team_id": [],
            "p1": [],
            "p2": [],
            "p3": [],
            "goalie": [],
            "shot_type": [],
            "x": [],
            "y": [],
            "reason": [],
            "penalty_duration": [],
        }

        for play in self.plays:
            df["n"].append(play.n)
            df["event_type"].append(event_type_to_string(play.event_type))
            df["period"].append(play.period)
            df["period_type"].append(play.period_type)
            df["time_in_period"].append(play.time_in_period)
            df["time_remaining"].append(play.time_remaining)
            df["event_owner_team_id"].append(play.event_owner_team_id)
            df["p1"].append(play.p1)
            df["p2"].append(play.p2)
            df["p3"].append(play.p3)
            df["goalie"].append(play.goalie)
            df["shot_type"].append(shot_type_to_string(play.shot_type))
            df["x"].append(play.x)
            df["y"].append(play.y)
            df["reason"].append(play.reason)
            df["penalty_duration"].append(play.penalty_duration)
        return pl.DataFrame(df).with_columns(pl.lit(self.game_id).alias("game_id"))
    

class NHLJsonPbpParser():

    def __init__(self) -> None:
        pass
    
    def parse(self, game_id: str) -> Game:
        url = "https://api-web.nhle.com/v1/gamecenter/" + str(game_id) + "/play-by-play" 
        
        try:
            json_res = requests.get(url).json()
        except:
            raise(RuntimeError(f"json pbb for game_id: {game_id} not found (url: {url})"))
        

        try:
            home_coach = json_res["summary"]["gameInfo"]["homeTeam"]["headCoach"]["default"]
        except:
            home_coach = None

        try:
            away_coach = json_res["summary"]["gameInfo"]["awayTeam"]["headCoach"]["default"]
        except:
            away_coach = None

        try:
            referee_1 = json_res["summary"]["gameInfo"]["referees"][0]["default"]
        except:
            referee_1 = None

        
        try:
            referee_2 = json_res["summary"]["gameInfo"]["referees"][1]["default"]
        except:
            referee_2 = None

        try: 
            linesmen_1 = json_res["summary"]["gameInfo"]["linesmen"][0]["default"]
        except:
            linesmen_1 = None

        try:
            linesmen_2 = json_res["summary"]["gameInfo"]["linesmen"][1]["default"]
        except:
            linesmen_2 = None
        


        game = Game(
            game_id = json_res["id"],
            season = json_res["season"],
            date = json_res["gameDate"],
            away_team = Team (
                name = json_res["awayTeam"]["commonName"]["default"],
                abrv = json_res["awayTeam"]["abbrev"],
                id = int(json_res["awayTeam"]["id"])
            ),
            home_team = Team (
                name = json_res["homeTeam"]["commonName"]["default"],
                abrv =  json_res["homeTeam"]["abbrev"],
                id = int(json_res["homeTeam"]["id"])
            ),
            away_team_goals = int(json_res["awayTeam"]["score"]),
            home_team_goals = int(json_res["homeTeam"]["score"]),
            venue = json_res["venue"]["default"],
            venue_location = json_res["venueLocation"]["default"],
            home_coach = home_coach,
            away_coach = away_coach,
            referee_1 = referee_1,
            referee_2 = referee_2,
            linesmen_1 = linesmen_1,
            linesmen_2 = linesmen_2,
            players = [],
            plays = []
        )

        for plr in json_res["rosterSpots"]:
            game.players.append(Player(
                first_name = plr["firstName"]["default"],
                last_name = plr["lastName"]["default"],
                id = int(plr["playerId"]),
                position = string_to_player_position(plr["positionCode"]),
                team_id = int(plr['teamId']),
                sweater_number = int(plr["sweaterNumber"])
            ))
        
        for play, i in zip(json_res["plays"], range(len(json_res["plays"]))):
            period = play["periodDescriptor"]["number"]
            period_type = play["periodDescriptor"]["periodType"]
            time_in_period = play["timeInPeriod"]
            time_remaining = play["timeRemaining"]
            event_type = string_to_event_type(play['typeDescKey'])

            if "details" in play.keys():
                if "winningPlayerId" in play["details"].keys(): p1 = int(play["details"]["winningPlayerId"])
                elif "shootingPlayerId" in play["details"].keys(): p1 = int(play["details"]["shootingPlayerId"])
                elif "hittingPlayerId" in play["details"].keys(): p1 = int(play["details"]["hittingPlayerId"])
                elif "playerId" in play["details"].keys(): p1 = int(play["details"]["playerId"])
                elif "scoringPlayerId" in play["details"].keys(): p1 = int(play["details"]["scoringPlayerId"])
                elif "committedByPlayerId" in play["details"].keys(): p1 = int(play["details"]["committedByPlayerId"])
                elif "shootingPlayerId" in play["details"].keys(): p1 = int(play["details"]["shootingPlayerId"])
                else: p1 = None

                if "losingPlayerId" in play["details"].keys(): p2 = int(play["details"]["losingPlayerId"])
                elif "hitteePlayerId" in play["details"].keys(): p2 = int(play["details"]["hitteePlayerId"])
                elif "blockingPlayerId" in play["details"].keys(): p2 = int(play["details"]["blockingPlayerId"])
                elif "assist1PlayerId" in play["details"].keys(): p2 = int(play["details"]["assist1PlayerId"])
                elif "drawnByPlayerId" in play["details"].keys(): p2 = int(play["details"]["drawnByPlayerId"])
                else: p2 = None

                if "assist2PlayerId" in play["details"].keys(): 
                    p3 = int(play["details"]["assist2PlayerId"])
                else: p3 = None
            
                if 'goalieInNetId' in play["details"].keys(): 
                    goalie = int(play["details"]["goalieInNetId"])
                else: goalie = None
                
                if 'xCoord' in play["details"].keys(): 
                    x = int(play["details"]["xCoord"])
                else: x = None
                
                if 'yCoord' in play["details"].keys(): 
                    y = int(play["details"]["yCoord"])
                else: y = None
                

                if 'reason' in play["details"].keys(): 
                    reason = play["details"]["reason"]
                else: reason = None
                
                if 'duration' in play["details"].keys(): 
                    penalty_duration = int(play["details"]["duration"])
                else: penalty_duration = None

                if 'eventOwnerTeamId' in play["details"].keys():
                    event_owner_team_id = int(play["details"]["eventOwnerTeamId"])
                else: event_owner_team_id = None

                if 'shotType' in play["details"].keys(): 
                    shot_type = string_to_shot_type(play["details"]['shotType'])
                else: shot_type = None 
            else:
                p1 = None
                p2 = None
                p3 = None
                goalie = None
                x = None
                y = None
                reason = None
                penalty_duration = None
                event_owner_team_id = None
                shot_type = None
            
            game.plays.append(Play(
                n = i,
                event_type = event_type,
                period = period,
                period_type = period_type,
                time_in_period = time_in_period,
                time_remaining = time_remaining,
                event_owner_team_id = event_owner_team_id,
                p1 = p1,
                p2 = p2,
                p3 = p3, 
                goalie = goalie,
                shot_type = shot_type,
                x = x,
                y = y,
                penalty_duration = penalty_duration,
                reason = reason
            ))
        
        return game
    
    # taking a list of Game objects and converting them into a 
    # dictionary of polars table to do with whatever your
    # heart desires
    def to_df(self, games: list[Game]) -> dict[str, pl.DataFrame]:
        return {
            "json_pbp_game_info": (
                pl.concat(map(lambda x: x.game_info_to_df(), games), how = "diagonal_relaxed")
            ),
            "json_pbp_player_info": (
                pl.concat(map(lambda x: x.players_to_df(), games), how = "diagonal_relaxed")
            ),
            "json_pbp_plays": (
                pl.concat(map(lambda x: x.plays_to_df(), games), how = "diagonal_relaxed")
            )
        }


def string_to_shot_type(s: str) -> ShotType:
    match s:
        case 'wrist': return(ShotType.Wrist)
        case 'slap': return(ShotType.Slap)
        case 'backhand': return(ShotType.Backhand)
        case 'snap': return(ShotType.Snap)
        case 'tip-in': return(ShotType.TipIn)
        case 'deflected': return(ShotType.Deflected)
        case 'wrap-around': return(ShotType.WrapAround)
        case 'between-legs': return(ShotType.BetweenLegs)
        case 'bat': return(ShotType.Bat)
        case 'poke': return(ShotType.Poke)
        case 'cradle': return(ShotType.Cradle)
        case _: raise(ValueError(s + " is not a valid ShotType"))

def shot_type_to_string(shot_type: ShotType | None) -> str | None:
    match shot_type:
        case ShotType.Wrist: return('wrist')
        case ShotType.Slap: return('slap')
        case ShotType.Backhand: return('backhand')
        case ShotType.Snap : return('snap')
        case ShotType.TipIn: return('tip-in')
        case ShotType.Deflected: return('deflected')
        case ShotType.WrapAround: return('wrap-around')
        case ShotType.BetweenLegs: return('between-legs')
        case ShotType.Bat: return('bat')
        case ShotType.Poke: return('poke')
        case ShotType.Cradle: return('cradle')
        case _: None

def string_to_event_type(s: str) -> EventType:
    match s:
        case 'period-start': return(EventType.PeriodStart)
        case 'faceoff': return(EventType.Faceoff)
        case 'shot-on-goal': return(EventType.ShotOnGoal)
        case 'stoppage': return(EventType.Stoppage)
        case 'missed-shot': return(EventType.MissedShot)
        case 'hit': return(EventType.Hit)
        case 'blocked-shot': return(EventType.BlockedShot)
        case 'giveaway': return(EventType.Giveaway)
        case 'takeaway': return(EventType.Takeaway)
        case 'goal': return(EventType.Goal)
        case 'penalty': return(EventType.Penalty)
        case 'delayed-penalty': return(EventType.DelayedPenalty)
        case 'period-end': return(EventType.PeriodEnd)
        case 'game-end': return(EventType.GameEnd)
        case 'shootout-complete': return(EventType.ShootoutComplete)
        case 'failed-shot-attempt': return(EventType.FailedShotAttempt)
        case _: raise(ValueError(s + "is not a valid EventType"))

def event_type_to_string(event_type: EventType | None) -> str | None:
    match event_type:
            case EventType.PeriodStart: return('period-start')
            case EventType.Faceoff: return('faceoff')
            case EventType.ShotOnGoal: return('shot-on-goal')
            case EventType.Stoppage: return('stoppage')
            case EventType.MissedShot: return('missed-shot')
            case EventType.Hit: return('hit')
            case EventType.BlockedShot: return('blocked-shot')
            case EventType.Giveaway: return('giveaway')
            case EventType.Takeaway: return('takeaway')
            case EventType.Goal: return('goal')
            case EventType.Penalty: return('penalty')
            case EventType.DelayedPenalty: return('delayed-penalty')
            case EventType.PeriodEnd: return('period-end')
            case EventType.GameEnd: return('game-end')
            case EventType.ShootoutComplete: return('shootout-complete')
            case EventType.FailedShotAttempt: return('failed-shot-attempt')
            case _ : None


def string_to_player_position(s: str) -> PlayerPosition:
    match s:
        case 'C': return(PlayerPosition.C)
        case 'L': return(PlayerPosition.L)
        case 'R': return(PlayerPosition.R)
        case 'D': return(PlayerPosition.D)
        case 'G': return(PlayerPosition.G)
        case _: raise(ValueError(s + "is not a valid PlayerPosition"))


def player_position_to_string(player_position: PlayerPosition | None) -> str | None:
    match player_position:
        case PlayerPosition.C: return('C')
        case PlayerPosition.L: return('L')
        case PlayerPosition.R: return('R')
        case PlayerPosition.D: return('D')
        case PlayerPosition.G: return('G')
        case _: return None


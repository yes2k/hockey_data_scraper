create database if not exists nhl_api_data;
create database if not exists xg;
create database if not exists win_model;

use nhl_api_data;

SET GLOBAL local_infile = true;


-- Removing and create tables
drop table if exists html_pbp_plays;
create table html_pbp_plays(
    game_id INT,
    n INT,
    period INT,
    strength VARCHAR(2),
    time_elapsed VARCHAR(5),
    event VARCHAR(20),
    description TEXT,
    away_on_ice_p1 VARCHAR(2),
    away_on_ice_p2 VARCHAR(2),
    away_on_ice_p3 VARCHAR(2),
    away_on_ice_p4 VARCHAR(2),
    away_on_ice_p5 VARCHAR(2),
    away_on_ice_p6 VARCHAR(2),
    away_on_ice_p7 VARCHAR(2),
    away_on_ice_p8 VARCHAR(2),
    away_on_ice_p9 VARCHAR(2),
    home_on_ice_p1 VARCHAR(2),
    home_on_ice_p2 VARCHAR(2),
    home_on_ice_p3 VARCHAR(2),
    home_on_ice_p4 VARCHAR(2),
    home_on_ice_p5 VARCHAR(2),
    home_on_ice_p6 VARCHAR(2), 
    home_on_ice_p7 VARCHAR(2),
    home_on_ice_p8 VARCHAR(2),
    home_on_ice_p9 VARCHAR(2),
    PRIMARY KEY (game_id, n)
);

drop table if exists json_pbp_game_info;
create table json_pbp_game_info (
    game_id INT,
    season INT,
    date DATE,
    away_team_name VARCHAR(30),
    away_team_abrv VARCHAR(3),
    away_team_id INT,
    away_team_goals INT,
    home_team_name VARCHAR(30),
    home_team_abrv VARCHAR(3),
    home_team_id INT,
    home_team_goals INT,
    venue VARCHAR(50),
    venue_location VARCHAR(50),
    referee_1 VARCHAR(50),
    referee_2 VARCHAR(50),
    linesmen_1 VARCHAR(50),
    linesmen_2 VARCHAR(50),
    home_coach VARCHAR(50),
    away_coach VARCHAR(50),
    PRIMARY KEY (game_id)
);

drop table if exists json_pbp_player_info;
create table json_pbp_player_info (
    team_id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    id INT,
    position VARCHAR(1),
    sweater_number VARCHAR(2),
    game_id INT,
    PRIMARY KEY (team_id, id, game_id)
);

drop table if exists json_pbp_plays;
create table json_pbp_plays (
    n INT,
    event_type VARCHAR(40),
    period INT,
    period_type VARCHAR(10),
    time_in_period VARCHAR(5),
    time_remaining VARCHAR(5),
    event_owner_team_id INT,
    p1 INT,
    p2 INT,
    p3 INT,
    goalie INT,
    shot_type VARCHAR(20),
    x INT,
    y INT,
    reason TEXT,
    penalty_duration INT,
    game_id INT,
    PRIMARY KEY (game_id, n)
);

drop table if exists json_shift_info;
create table json_shift_game_info (
    game_id INT,
    id INT,
    start_time VARCHAR(5),
    end_time VARCHAR(5),
    period INT,
    duration VARCHAR(5),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    player_id INT,
    team_id INT,
    team_abbrev VARCHAR(3),
    PRIMARY KEY (game_id, id, player_id, team_id, period, start_time, end_time)
);

-- Loading tables from csv
LOAD DATA LOCAL INFILE './csvs/html_pbp_plays.csv'
INTO TABLE html_pbp_plays
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE './csvs/json_pbp_game_info.csv'
INTO TABLE json_pbp_game_info
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE './csvs/json_pbp_player_info.csv'
INTO TABLE json_pbp_player_info
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE './csvs/json_pbp_plays.csv'
INTO TABLE json_pbp_plays
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

LOAD DATA LOCAL INFILE './csvs/json_shift_info.csv'
INTO TABLE json_shift_info
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
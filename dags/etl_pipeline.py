
from __future__ import annotations

import json
import pendulum
import pandas
import requests
import statsapi

from airflow.decorators import dag, task
from sqlalchemy import create_engine


@dag(
    schedule=None,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=["etl"],
)
def etl_pipeline():

    conn_string = 'postgresql://airflow:airflow@baseball-database/airflow'
    engine = create_engine(conn_string)

    @task()
    def extract_raw(source="database"):
        """
        #### Extract task
        Extract raw data from the mlb stats api in some cases or just from
        the "raw" table in the baseball database.
        """
        if source == "api":
            start_date = '01/01/2022'
            end_date = '12/15/2022'
            team_ids = pandas.read_sql("""SELECT team_id FROM teams WHERE full_name NOT LIKE '%All%'""", engine)["team_id"]

            df = None
            for team_id in team_ids:
                print(f"Gathering team id: {team_id}")
                temp = pandas.DataFrame(statsapi.schedule(
                    team=team_id,
                    start_date=start_date,
                    end_date=end_date,
                ))
                if df is None:
                    df = temp
                else:
                    df = pandas.concat([df, temp]).drop_duplicates(["game_id"])
                time.sleep(2)

            df.to_sql('raw', engine, if_exists='replace', index=False)


        elif source == "database":
            df = pandas.read_sql("SELECT * FROM raw", engine)
            load('raw2', df)
            return "raw"

        raise ValueError(f"Unknown source for extract step: {source}")

    @task()
    def generate_venues(raw):
        raw = pandas.read_sql(f"SELECT * FROM raw", engine)
        venues_df = raw.drop_duplicates(["venue_id", "venue_name"])[["venue_id", "venue_name"]]
        load('venues', venues_df)
        return 'venues'

    @task()
    def generate_home_venues(schedule):
        raw = pandas.read_sql(f"SELECT * FROM schedule", engine)
        counts = schedule.groupby(['home_id', 'venue_id']).size().reset_index(name='counts')
        home_venues = counts[counts["counts"] > 60].reset_index()
        home_venues["team_id"] = home_venues["home_id"]
        venues_df = home_venues[["team_id", "venue_id"]]
        load('home_venues', venues_df)
        return 'home_venues'

    @task()
    def generate_schedule(raw):
        raw = pandas.read_sql(f"SELECT * FROM raw", engine)
        df = raw[[
            "game_id", "game_datetime", "game_date", "game_num",
            "away_id", "home_id", "venue_id"
        ]]
        load('schedule', df)

    @task()
    def generate_scores(raw):
        raw = pandas.read_sql(f"SELECT * FROM raw", engine)
        finalized = raw[raw["status"] == "Final"][["game_id", "away_score", "home_score", "winning_team", "losing_team"]]
        load('scores', finalized)
        return 'scores'

    @task()
    def generate_record(scores):
        df = pandas.read_sql("""
            SELECT teams.team_id, record.wins, record.losses FROM
            (
                SELECT wins.winning_team AS team_name, wins.wins, losses.losses
                FROM
                (SELECT winning_team, COUNT(*) AS wins
                FROM scores GROUP BY winning_team) as wins
                JOIN
                (SELECT losing_team, COUNT(*) AS losses
                FROM scores GROUP BY losing_team) as losses
                ON losses.losing_team = wins.winning_team
                ORDER BY wins.winning_team
            ) AS record LEFT JOIN teams
            ON teams.full_name = record.team_name
        """, engine)
        load('record', df)
        return 'record'

    @task()
    def load(table_name, df):
        """
        #### Load task
        A simple Load task which takes writes the given dataframe into a table
        in the database, replacing if exists.
        """
        df.to_sql(table_name, engine, if_exists='replace', index=False)


        

    # [START main_flow]
    raw = extract_raw(source="database")
    # raw = extract_raw(source="database")
    venues = generate_venues(raw)
    schedule = generate_schedule(raw)
    scores = generate_scores(raw)
    #home_venues = generate_home_venues(schedule)
    record = generate_record(scores)

    # [END main_flow]


etl_pipeline()



from __future__ import annotations

import json
import pendulum
import pandas
import requests
import statsapi
import psycopg2

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
        
        # drop the existing tables except raw and teams
        with psycopg2.connect(conn_string) as conn:
            with conn.cursor() as cursor:
                cursor.execute("drop table if exists schedule cascade")
                cursor.execute("drop table if exists scores cascade")
                cursor.execute("drop table if exists venues cascade")
                conn.commit()
        
        if source == "api":
            start_date = '01/01/2022'
            end_date = '12/15/2022'
            team_ids = pandas.read_sql("""SELECT team_id FROM teams WHERE full_name NOT LIKE '%%All%%'""", engine)["team_id"]

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

            # df.to_sql('raw2', engine, if_exists='replace', index=False)


        elif source == "database":
            df = pandas.read_sql("SELECT * FROM raw", engine)
            # df.to_sql('raw2', engine, if_exists='replace', index=False)
            return "raw"

        raise ValueError(f"Unknown source for extract step: {source}")

    @task()
    def generate_venues(raw):
        raw_df = pandas.read_sql(f"SELECT * FROM raw", engine)
        venues_df = raw_df.drop_duplicates(["venue_id", "venue_name"])[["venue_id", "venue_name"]]
        venues_df.to_sql('venues', engine, if_exists='replace', index=False)
        return 'venues'

    @task()
    def generate_home_venues(*args):
        schedule_df = pandas.read_sql(f"SELECT * FROM schedule", engine)
        counts = schedule_df.groupby(['home_id', 'venue_id']).size().reset_index(name='counts')
        home_venues = counts[counts["counts"] > 60].reset_index()
        home_venues["team_id"] = home_venues["home_id"]
        venues_df = home_venues[["team_id", "venue_id"]]
        venues_df.to_sql('home_venues', engine, if_exists='replace', index=False)
        return 'home_venues'

    @task()
    def generate_schedule(*args):
        raw_df = pandas.read_sql(f"SELECT * FROM raw", engine)
        df = raw_df[[
            "game_id", "game_datetime", "game_date", "game_num",
            "away_id", "home_id", "venue_id"
        ]]
        df.to_sql('schedule', engine, if_exists='replace', index=False)
        return 'schedule'

    @task()
    def generate_scores(*args):
        raw_df = pandas.read_sql(f"SELECT * FROM raw", engine)
        finalized = raw_df[raw_df["status"] == "Final"][["game_id", "away_score", "home_score", "winning_team", "losing_team"]]
        finalized.to_sql('scores', engine, if_exists='replace', index=False)
        return 'scores'

    @task()
    def generate_record(*args):
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
        df.to_sql('record', engine, if_exists='replace', index=False)
        return 'record'



        

    # [START main_flow]
    raw = extract_raw(source="database")
    venues = generate_venues(raw)
    schedule = generate_schedule(raw)
    scores = generate_scores(raw)
    home_venues = generate_home_venues(schedule)
    record = generate_record(scores)

    # [END main_flow]


etl_pipeline()


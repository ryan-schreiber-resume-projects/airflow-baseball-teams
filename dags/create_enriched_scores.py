
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
    tags=["analytics"],
)
def create_enriched_scores():

    conn_string = 'postgresql://airflow:airflow@baseball-database/airflow'
    engine = create_engine(conn_string)

    @task()
    def generate_enriched_scores(*args):
        team_ids = pandas.read_sql("""SELECT team_id FROM teams WHERE full_name NOT LIKE '%%All%%'""", engine)["team_id"]
        df = None
        for team_id in team_ids:
            temp = pandas.read_sql(f"""
                with cte as (
                        select 
                                schedule.game_id,
                                DATE(schedule.game_date) as game_date , 
                                {team_id} as team_id,
                                case when schedule.home_id = {team_id} then 'home' else 'away' end as home_away,
                                case when schedule.home_id = {team_id} then scores.home_score  else scores.away_score end as runs_scored,
                                case when schedule.home_id = {team_id} then scores.away_score  else scores.home_score end as runs_allowed,
                                lag(DATE(schedule.game_date), 1) over(order by schedule.game_date) as previous_game_date			
                        from schedule join scores
                        on schedule.game_id = scores.game_id 
                        where schedule.home_id = {team_id} or schedule.away_id = {team_id}
                        order by game_date
                )
                select 
                        team_id,
                        game_date,
                        runs_scored,
                        runs_allowed,
                        lag(home_away, 1) over(order by game_date) <> home_away as travel_before,
                        game_date <> (cast(previous_game_date as date) + INTERVAL '1 day') as rest_day_before
                from cte
                where game_date >= '2022-04-07'
            """, engine)
            if df is None:
                df = temp
            else:
                df = pandas.concat([df, temp])
        df.to_sql('enriched_scores', engine, if_exists='replace', index=False)


    # [START main_flow]
    generate_enriched_scores()
    # [END main_flow]


create_enriched_scores()


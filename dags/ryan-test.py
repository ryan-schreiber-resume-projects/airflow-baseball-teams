#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

# [START tutorial]
# [START import_module]
import json

import pendulum

from airflow.decorators import dag, task

# testing that requirements.txt worked
import pandas
import requests
import pyspark

# [END import_module]


# [START instantiate_dag]
@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["ryan"],
)
def ryan_test():
    """
    ### TaskFlow API Tutorial Documentation
    This is a simple data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.
    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html)
    """
    # [END instantiate_dag]

    # [START extract]
    @task()
    def extract():
        """
        #### Extract task
        A simple Extract task to get data ready for the rest of the data
        pipeline. In this case, getting data is simulated by reading from a
        hardcoded JSON string.
        """
        
        # url = "https://bdfed.stitch.mlbinfra.com/bdfed/transform-mlb-schedule?stitch_env=prod&sortTemplate=5&sportId=1&&sportId=51&startDate=2023-07-01&endDate=2023-07-02&gameType=E&&gameType=S&&gameType=R&&gameType=F&&gameType=D&&gameType=L&&gameType=W&&gameType=A&language=en&leagueId=104&&leagueId=103&&leagueId=160&contextTeamId="
        # response = requests.get(url)
        # if response.ok:
            # print(list(response.json().keys()))
        # else:
            # print(response.text)
        
        data = [{"a" : i, "b": i ** 2, "c": i ** 3} for i in range(1,10)]
        
        df = pandas.DataFrame.from_dict(data)
        df.head()
        
        spark = SparkSession.builder.config(conf=SparkConf()).getOrCreate()
        df = spark.createDataFrame(df)
        print(df.schema)
        
        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'

        order_data_dict = json.loads(data_string)
        return order_data_dict

    # [END extract]

    # [START transform]
    @task(multiple_outputs=True)
    def transform(order_data_dict: dict):
        """
        #### Transform task
        A simple Transform task which takes in the collection of order data and
        computes the total order value.
        """
        total_order_value = 0

        for value in order_data_dict.values():
            total_order_value += value

        return {"total_order_value": total_order_value}

    # [END transform]

    # [START load]
    @task()
    def load(total_order_value: float):
        """
        #### Load task
        A simple Load task which takes in the result of the Transform task and
        instead of saving it to end user review, just prints it out.
        """

        print(f"Total order value is: {total_order_value:.2f}")

    # [END load]

    # [START main_flow]
    order_data = extract()
    order_summary = transform(order_data)
    load(order_summary["total_order_value"])
    # [END main_flow]


# [START dag_invocation]
ryan_test()
# [END dag_invocation]

# [END tutorial]
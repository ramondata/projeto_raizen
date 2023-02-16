#!/usr/bin/env python
# -*- encoding: utf-8 -*-

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

with DAG (
        dag_id = "get_parquet",
        start_date = days_ago(1),
        schedule_interval = "@daily"
        ) as dag:

    task_1 = BashOperator(
            task_id = "path_file_spark",
            bash_command = "cd /home/ramon/projeto_raizen/local_spark/")

    task_2 = BashOperator(
            task_id = "simulator_spark_in_python",
            bash_command = "python parquet_build.py")


    task_1 >> task_2

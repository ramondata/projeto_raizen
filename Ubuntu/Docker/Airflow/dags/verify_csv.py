#!/usr/bin/env python3
# -*- encoding: utf-8 -*-

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

with DAG (
        dag_id = "verify_csv_file",
        start_date = days_ago(1),
        schedule_interval = "@daily"
        ) as dag:

    task_1 = BashOperator(
            task_id = "check_exist_file",
            bash_command = "cd /home/ramon/projeto_raizen/; mkdir -p test_new_dir"
            )
    
    def printar_word():
        print('airflow is amazing')


    task_2 = PythonOperator(
            task_id = "Python_print",
            python_callable = printar_word
            )

    task_1 >> task_2

import time
from datetime import datetime
# from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
# from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.hooks.base_hook import BaseHook
import pandas as pd
from sqlalchemy import create_engine



from datetime import datetime

from airflow import DAG
# from airflow.decorators import task
from airflow.operators.bash import BashOperator

# A DAG represents a workflow, a collection of tasks
with DAG(dag_id="automate_etl_with_airflow_test", start_date=datetime(2022, 1, 1), schedule="0 0 * * *") as dag:

    # Tasks are represented as operators
    hello = BashOperator(task_id="hello", bash_command="echo 'hello'")  # pyright: ignore[reportUndefinedVariable]

    @task()
    def airflow():
        # print("airflow")
         ## database: test_db,  User: test_user, pwd: test,  table: tbl_test
        conn = BaseHook.get_connection('postgres_test')
        engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
        pc = pd.read_sql_query('SELECT * FROM tbl_test', engine)
        print(pc)
        return pc

    # Set dependencies between tasks
    hello >> airflow()
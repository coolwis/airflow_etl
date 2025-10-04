import time
from datetime import datetime
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
# from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.hooks.base_hook import BaseHook
import pandas as pd
from sqlalchemy import create_engine

#extract tasks
@task()
def get_src_tables():
    # hook = MsSqlHook(mssql_conn_id="sqlserver")
    # sql = """ select  t.name as table_name  
    #  from sys.tables t where t.name in ('DimProduct','DimProductSubcategory','DimProductCategory') """
    # df = hook.get_pandas_df(sql)
    # print(df)
    # tbl_dict = df.to_dict('dict')
    # return tbl_dict

    ## database: test_db,  User: test_user, pwd: test,  table: tbl_test
    conn = BaseHook.get_connection('postgres_test')
    engine = create_engine(f'postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}')
    pc = pd.read_sql_query('SELECT * FROM tbl_test', engine)
    print(pc)
    return pc

# [START how_to_task_group]
with DAG(dag_id="product_etl_dag",schedule_interval="0 9 * * *", start_date=datetime(2022, 3, 5),catchup=False,  tags=["product_model"]) as dag:

    with TaskGroup("extract_dimProudcts_load", tooltip="Extract and load source data") as extract_load_src:
        src_product_tbls = get_src_tables()
        #define order
        src_product_tbls

    extract_load_src
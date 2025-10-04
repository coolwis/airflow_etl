from datetime import datetime
from airflow.sdk import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.operators.subdag import SubDagOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import PythonOperator


@dag(start_date=datetime(2025, 1, 1), schedule='@daily', catchup=False)
def my_dag():
    # accurates = []
    @task
    def train_model_a():
        print("Training model A")
        return 1
    
    @task
    def train_model_b():
        print("Training model B")
        return 2
    
    @task
    def train_model_c():
        print("Training model C")
        return 3

    @task.branch   
    def choose_best_model(accurates: list[int]):
        print("Choosing best model")
        if max(accurates) > 2:
            return "accurate"
        else:
            return "inaccurate"

    @task.bash
    def accurate():
        return "echo 'accurate'"    
    @task.bash
    def inaccurate():
        return "echo 'inaccurate'"

    accurates = [train_model_a() , train_model_b() , train_model_c()] 
    choose_best_model(accurates) >> [accurate(), inaccurate()]
    
my_dag()

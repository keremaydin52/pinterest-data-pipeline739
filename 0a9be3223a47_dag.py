from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from airflow.operators.bash_operator import BashOperator

#Define params for Submit Run Operator
notebook_task = {
    'notebook_path': '/Users/keremaydin52@gmail.com/databricks'
}


#Define params for Run Now Operator
notebook_params = {
    "Variable":5
}

default_args = {
    'owner': 'Kerem',
    'depends_on_past': False,
    'email': ['keremaydin52@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date': datetime(2024, 2, 11),
    'retry_delay': timedelta(minutes=5),
    'end_date': datetime(2025, 1, 1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'trigger_rule': 'all_success'
}

with DAG(dag_id='0a9be3223a47_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False,
         tags=['Databricks']
         ) as dag:
    
    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
        # the connection we set-up previously
        databricks_conn_id='databricks_default',
        existing_cluster_id='1108-162752-8okw8dgg',
        notebook_task=notebook_task
    )
    opr_submit_run
    
    '''
    # Define the tasks. Here we are going to define only one bash operator
    test_task = BashOperator(
        task_id='write_date_file',
        bash_command='cd ~/Desktop && date >> ai_core.txt',
        dag=dag)
    '''
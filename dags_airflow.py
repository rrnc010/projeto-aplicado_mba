import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2)
}


dag = DAG(
    dag_id='atualiza_base_ans',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    dagrun_timeout=timedelta(minutes=60),
    tags=['date_file']
)

t1 = BashOperator(
    task_id='extracao_arquivo',
    bash_command="python extracao_indice_reclamacao.py",
    dag=dag
)

t2 = BashOperator(
    task_id='atualizacao_camadas_bigquery',
    bash_command="python criar_camadas_gcp.py",
    dag=dag
)


t1 >> t2 #Fluxo de execução das tasks
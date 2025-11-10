"""
DAG para ETL dos dados públicos de CNPJ da Receita Federal do Brasil
Autor: Aphonso Henrique (Adaptado para Airflow)
Descrição: Executa o script ETL_coletar_dados_e_gravar_BD.py
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Argumentos padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definir a DAG
dag = DAG(
    'etl_receita_federal',
    default_args=default_args,
    description='ETL completo dos dados públicos de CNPJ da Receita Federal',
    schedule_interval='@monthly',  # Executar mensalmente
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['receita_federal', 'cnpj', 'etl'],
)

# Task única: Executar o script ETL completo
run_etl = BashOperator(
    task_id='executar_etl_receita_federal',
    bash_command='cd /opt/airflow/etl_scripts && python ETL_coletar_dados_e_gravar_BD.py',
    dag=dag,
)


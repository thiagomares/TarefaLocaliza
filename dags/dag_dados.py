import os
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from datetime import datetime, timedelta
from airflow.hooks.base_hook import BaseHook
import gdown
from dotenv import load_dotenv

load_dotenv()


default_args = {
    'owner': 'Thiago Mares',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)}



# Eu estou usando um hook para fazer log de cada item no processo, pois eu preciso entender se o processo está funcionando corretamente
with DAG(
    dag_id="ProjetoLocaliza",
    default_args=default_args,
    description="Tarefa para engenheiro de dados da localiza",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    
) as dag:

    
    DummyOperator = DummyOperator(
        task_id="start",
        dag=dag,
    )

    criar_tabela_logs = MySqlOperator(
        task_id='check_status',
        sql=""" 
        CREATE TABLE IF NOT EXISTS logs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            task_name VARCHAR(100),
            status VARCHAR(20),
            message TEXT,
            dag_name VARCHAR(100),
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
        trigger_rule='one_failed',
        mysql_conn_id='mysql_default',
    )

    # se eu tiver um pouco mais de tempo eu implemento esse log melhor, mas por enquanto ele só vai inserir o status da tarefa no banco de dados
    def log_to_db(task_name, status, message, dag_id):
        mysql_conn = BaseHook.get_connection('mysql_default')
        sql = f"""
        INSERT INTO logs (task_name, status, message, dag_name)
        VALUES ('{task_name}', '{status}', '{message}', '{dag_id}');
        """
        
        log_task = MySqlOperator(
            task_id=f'log_{task_name}',
            sql=sql,
            mysql_conn_id='mysql_default',
            dag=dag,
        )
        
        log_task.execute(context={})

    teste = SparkSubmitOperator(
        task_id="tabela_clientes",
        conn_id="spark_default",
        application="./include/teste.py",
        packages='mysql:mysql-connector-java:8.0.32,com.amazonaws:aws-java-sdk-s3:1.12.200,org.apache.hadoop:hadoop-aws:3.3.1',
        dag=dag,
        on_failure_callback=[log_to_db('falha_validação', 'failed', 'Erro ao processar a tabela de clientes', 'ProjetoLocaliza')],
        on_success_callback=[log_to_db('sucesso_validação', 'success', 'Tabela de clientes processada com sucesso', 'ProjetoLocaliza')]
    )

    DummyOperator >> teste

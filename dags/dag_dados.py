import os
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from airflow.hooks.base_hook import BaseHook
import gdown


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

    
    Start = DummyOperator(
        task_id="start",
        dag=dag,
    )

    End = DummyOperator(
        task_id="end",
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
    
    # Preferi fazer tudo via spark pois é uma maneira mais fácil e rápida de fazer o processamento dos dados e implementar a qualidade dos dados, mas poderia ter feito via python e pandas, mas acredito que o spark é mais eficiente e o pandas nessa situação seria um pouco mais lento, devido ao volume de dados relativamente alto


    teste = SparkSubmitOperator(
        task_id="testeDados",
        conn_id="spark_default",
        application="./include/teste.py",
        packages='mysql:mysql-connector-java:8.0.32,com.amazonaws:aws-java-sdk-s3:1.12.200,org.apache.hadoop:hadoop-aws:3.3.1',
        dag=dag,
        on_failure_callback=[log_to_db('falha_validação', 'failed', 'Erro ao processar a tabela', 'ProjetoLocaliza')],
        on_success_callback=[log_to_db('sucesso_validação', 'success', 'Tabela de clientes processada com sucesso', 'ProjetoLocaliza')]
    )

    qualidade = SparkSubmitOperator(
        task_id="QualidadeDados",
        conn_id="spark_default",
        application="./include/qualidade_dados.py",
        packages='mysql:mysql-connector-java:8.0.32,com.amazonaws:aws-java-sdk-s3:1.12.200,org.apache.hadoop:hadoop-aws:3.3.1',
        dag=dag,
        on_failure_callback=[log_to_db('falha_Qualidade', 'failed', 'Erro ao processar a tabela', 'ProjetoLocaliza')],
        on_success_callback=[log_to_db('sucesso_Qualidade', 'success', 'Tabela de clientes processada com sucesso', 'ProjetoLocaliza')]
    )

    limpeza_dados = SparkSubmitOperator(
            task_id="limpeza_dados",
            conn_id="spark_default",
            application="./include/limpeza_dados.py",
            packages='mysql:mysql-connector-java:8.0.32,com.amazonaws:aws-java-sdk-s3:1.12.200,org.apache.hadoop:hadoop-aws:3.3.1',
            dag=dag,
            on_failure_callback=[log_to_db('falha_Limpeza', 'failed', 'Erro ao processar a tabela', 'ProjetoLocaliza')],
            on_success_callback=[log_to_db('sucesso_Limpeza', 'success', 'Tabela de clientes processada com sucesso', 'ProjetoLocaliza')]
        )

    location_region = SparkSubmitOperator(
            task_id="location_region",
            conn_id="spark_default",
            application="./include/location_region.py",
            packages='mysql:mysql-connector-java:8.0.32,com.amazonaws:aws-java-sdk-s3:1.12.200,org.apache.hadoop:hadoop-aws:3.3.1',
            dag=dag,
            on_failure_callback=[log_to_db('falha_region', 'failed', 'Erro ao processar a tabela', 'ProjetoLocaliza')],
            on_success_callback=[log_to_db('sucesso_region', 'success', 'Tabela processada com sucesso', 'ProjetoLocaliza')]
        )

    transaction = SparkSubmitOperator(
            task_id="transactions",
            conn_id="spark_default",
            application="./include/transacoes.py",
            packages='mysql:mysql-connector-java:8.0.32,com.amazonaws:aws-java-sdk-s3:1.12.200,org.apache.hadoop:hadoop-aws:3.3.1',
            dag=dag,
            on_failure_callback=[log_to_db('falha_transaction', 'failed', 'Erro ao processar a tabela', 'ProjetoLocaliza')],
            on_success_callback=[log_to_db('sucesso_transaction', 'success', 'Tabela processada com sucesso', 'ProjetoLocaliza')]
        )
    
    Start >> teste >> [qualidade, limpeza_dados]  
    limpeza_dados >> [location_region, transaction]
    [location_region, transaction, qualidade] >> End

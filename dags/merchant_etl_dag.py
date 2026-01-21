from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from etl.bronze import run_bronze

with DAG(
    dag_id="merchant_incremental_etl",
    start_date=days_ago(1),
    schedule_interval="*/5 * * * *",  # every 5 minutes
    catchup=False,
    max_active_runs=1
) as dag:

    bronze = PythonOperator(
        task_id="bronze_incremental",
        python_callable=run_bronze
    )

    silver = PostgresOperator(
        task_id="silver_transform",
        postgres_conn_id="postgres_default",  # 🔴 REQUIRED
        sql="sql/silver.sql"
    )

    gold = PostgresOperator(
        task_id="gold_transform",
        postgres_conn_id="postgres_default",  # 🔴 REQUIRED
        sql="sql/gold.sql"
    )

    bronze >> silver >> gold

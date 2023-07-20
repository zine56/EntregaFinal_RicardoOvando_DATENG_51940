# Este es el DAG que orquesta el ETL de la tabla users

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable

from datetime import datetime, timedelta

QUERY_CREATE_TABLE = '''
    CREATE TABLE IF NOT EXISTS "stock" (
        "ticker" VARCHAR(10) NOT NULL,
        "hour_key" VARCHAR(17) NOT NULL,
        "name" VARCHAR(100) NULL DEFAULT NULL,
        "exchange_short" VARCHAR(20) NULL DEFAULT NULL,
        "exchange_long" VARCHAR(100) NULL DEFAULT NULL,
        "mic_code" VARCHAR(100) NULL DEFAULT NULL,
        "currency" VARCHAR(10) NULL DEFAULT NULL,
        "price" FLOAT8 NULL DEFAULT NULL,
        "day_high" FLOAT8 NULL DEFAULT NULL,
        "day_low" FLOAT8 NULL DEFAULT NULL,
        "day_avg" FLOAT8 NULL DEFAULT NULL,
        "day_open" FLOAT8 NULL DEFAULT NULL,
        "52_week_high" FLOAT8 NULL DEFAULT NULL,
        "52_week_low" FLOAT8 NULL DEFAULT NULL,
        "market_cap" BIGINT NULL DEFAULT NULL,
        "previous_close_price" FLOAT8 NULL DEFAULT NULL,
        "previous_close_price_time" TIMESTAMP NULL DEFAULT NULL,
        "day_change" FLOAT8 NULL DEFAULT NULL,
        "volume" BIGINT NULL DEFAULT NULL,
        "is_extended_hours_price" BOOL NULL DEFAULT NULL,
        "last_trade_time" TIMESTAMP NULL DEFAULT NULL,
        "insertion_date" TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP,
        "batch_id" BIGINT NULL DEFAULT NULL,
        "price_percentage" FLOAT8 NULL DEFAULT NULL
    )
    DISTKEY("ticker")
    SORTKEY("hour_key");
'''

QUERY_CLEAN_PROCESS_DATE = """
    DELETE FROM "stock" WHERE "hour_key" = '{{ ti.xcom_pull(key="hour_key") }}'
"""

# create function to get process_date and push it to xcom
def get_process_date(**kwargs):
    # If process_date is provided, take it; otherwise, take today
    if (
        "process_date" in kwargs["dag_run"].conf
        and kwargs["dag_run"].conf["process_date"] is not None
    ):
        process_date = kwargs["dag_run"].conf["process_date"]
        hour_key = datetime.strptime(process_date, "%Y-%m-%d").strftime('%Y_%m_%d_%H_00')
    else:
        process_date = kwargs["dag_run"].conf.get(
            "process_date", datetime.now().strftime("%Y-%m-%d")
        )
        hour_key = datetime.now().strftime('%Y_%m_%d_%H_00')
    kwargs["ti"].xcom_push(key="process_date", value=process_date)
    kwargs["ti"].xcom_push(key="hour_key", value=hour_key)


default_args = {
    "owner": "Ricardo Ovando",
    "start_date": datetime(2023, 7, 1),
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
}

with DAG(
    dag_id="etl_stock",
    default_args=default_args,
    description="ETL de stock diario",
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    # Tareas
    get_process_date_task = PythonOperator(
        task_id="get_process_date",
        python_callable=get_process_date,
        provide_context=True,
    )

    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="redshift_default",
        sql=QUERY_CREATE_TABLE,
    )

    clean_process_date = PostgresOperator(
        task_id="clean_process_date",
        postgres_conn_id="redshift_default",
        sql=QUERY_CLEAN_PROCESS_DATE,
    )

    spark_etl_stock = SparkSubmitOperator(
        task_id="spark_etl_stock",
        application=f'{Variable.get("spark_scripts_dir")}/ETL_Stock.py',
        conn_id="spark_default",
        driver_class_path=Variable.get("driver_class_path"),
    )

    get_process_date_task >> create_table >> clean_process_date >> spark_etl_stock

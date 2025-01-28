
from airflow import DAG
import pandas
from datetime import datetime
from airflow.configuration import conf
from airflow.models import Variable 
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator 
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

PATH = Variable.get("my_path")
conf.set("core", "template_searchpath", PATH)

postgres_hook = PostgresHook("postgres-db")
engine = postgres_hook.get_sqlalchemy_engine()
date_now = datetime.now()

def import_(table_name):
    df = pandas.read_csv(PATH + f"{table_name}.csv", delimiter=",", encoding='utf-8')
    df.to_sql(table_name, engine, schema="stage", if_exists="replace", index=False) 


def uploading_logs(context):
    task_instance = context['task_instance'].task_id
    ts = context['task_instance'].execution_date.timestamp()
    ts = datetime.fromtimestamp(ts)
    log_schema = 'log'
    log_table = 'logt'
    query = f"""
        INSERT INTO {log_schema}.{log_table} (execution_datetime, event_datetime, event_name)
        VALUES ('{ts}', '{datetime.now().isoformat(sep='T')}', '{task_instance}');
    """
    postgres_hook.run(query)


default_args = {
    "owner" : "budanovsa",
    "start_date" : date_now,
    "retries" : 2
}
with DAG(
    "import_date",
    default_args=default_args,
    description="Загрузка CSV в базу",
    catchup=False,
    template_searchpath = [PATH],
    # schedule="0 0 * * *"
) as dag:
    start = DummyOperator(
        task_id="start",
        on_success_callback=uploading_logs
    )
    import_dm_f101_round_f = PythonOperator(
        task_id="import_dm_f101_round_f",
        python_callable=import_,
        op_kwargs={"table_name" : "dm_f101_round_f"},
        on_success_callback=uploading_logs
    )  
    split = DummyOperator(
        task_id="split",
        on_success_callback=uploading_logs
    )
    sql_dm_f101_round_f = SQLExecuteQueryOperator(
        task_id="sql_dm_f101_round_f",
        conn_id="postgres-db",
        sql="sql/dm_f101_round_f.sql",
        on_success_callback=uploading_logs
    )
    end = DummyOperator(
        task_id="end",
        on_success_callback=uploading_logs
    )

    {
        start
        >>[import_dm_f101_round_f]
        >>split
        >>[sql_dm_f101_round_f]
        >>end
    }

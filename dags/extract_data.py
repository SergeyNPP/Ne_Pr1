from airflow import DAG
from datetime import datetime
from airflow.configuration import conf
from airflow.models import Variable 
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator 
from airflow.providers.postgres.hooks.postgres import PostgresHook


postgres_hook = PostgresHook("postgres-db")
engine = postgres_hook.get_sqlalchemy_engine()

def extract(**kwargs):
    context = kwargs
    ts = context['task_instance'].execution_date.timestamp()
    ts = datetime.fromtimestamp(ts)
    tablename = 'dm.dm_f101_round_f'
    log_schema = 'log'
    log_table = 'logt'
    query = f"""
        COPY (SELECT * FROM {tablename}) TO 'D:/DE/VSCODE/AIRFLOW/Airflow/files/dm_f101_round_f.csv' WITH CSV HEADER;
        INSERT INTO {log_schema}.{log_table} (execution_datetime, event_datetime, event_name)
        VALUES ('{ts}', '{datetime.now().isoformat(sep='T')}', 'extract_{tablename}');
    """
    postgres_hook.run(query)

PATH = Variable.get("my_path")
conf.set("core", "template_searchpath", PATH)
date_now = datetime.now()

default_args = {
    "owner" : "budanovsa",
    "start_date" : date_now,
    "retries" : 2
}
with DAG(
    "extract_date",
    default_args=default_args,
    description="Выгрузка в CSV",
    catchup=False,
    template_searchpath = [PATH],
    schedule="0 0 * * *"
) as dag:
    start = DummyOperator(
        task_id="start",
    )
    extract_date = PythonOperator(
        task_id="extract_date",
        python_callable=extract,
    )
    end = DummyOperator(
        task_id="end",
    )
    {
        start
        >>[extract_date]
        >>end
    }

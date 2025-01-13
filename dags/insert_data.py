from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator 
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.configuration import conf
from airflow.models import Variable

from datetime import datetime 
from time import sleep
import pandas

PATH = Variable.get("my_path")
conf.set("core", "template_searchpath", PATH)
date_now = datetime.now()
postgres_hook = PostgresHook("postgres-db")
engine = postgres_hook.get_sqlalchemy_engine()


def insert_data(table_name):
    try:
        df = pandas.read_csv(PATH + f"{table_name}.csv", delimiter=";", encoding='utf-8')
    except UnicodeDecodeError:
        df = pandas.read_csv(PATH + f"{table_name}.csv", delimiter=";", encoding='Windows-1252')
    df.to_sql(table_name, engine, schema="stage", if_exists="replace", index=False) 
    # append
    # replace


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
    
    
def start_log(**kwargs):
    context = kwargs
    dag_run = context['dag_run'].run_id
    ts = context['dag_run'].execution_date.timestamp()
    ts = datetime.fromtimestamp(ts)
    log_schema = 'log'
    log_table = 'logt'
    with engine.connect() as connection:
        connection.execute(f"""
            INSERT INTO {log_schema}.{log_table} (execution_datetime, event_datetime, event_name)
            VALUES ('{ts}', '{datetime.now().isoformat(sep='T')}', '{"dag_run"}');
        """)
    sleep(5)



default_args = {
    "owner" : "budanovsa",
    "start_date" : date_now,
    "retries" : 2
}

with DAG(
    "insert_date",
    default_args=default_args,
    description="Загрузка данных в stage и log",
    catchup=False,
    template_searchpath = [PATH],
    schedule="0 0 * * *"
) as dag:

    start = DummyOperator(
        task_id="start",
        on_success_callback=uploading_logs
    )

    task_start_log = PythonOperator(
        task_id="start_log",
        python_callable=start_log,
    )
    
    ft_balance_f = PythonOperator(
        task_id="ft_balance_f",
        python_callable=insert_data,
        op_kwargs={"table_name" : "ft_balance_f"},
        on_success_callback=uploading_logs
    )

    ft_posting_f = PythonOperator(
        task_id="ft_posting_f",
        python_callable=insert_data,
        op_kwargs={"table_name" :  "ft_posting_f"},
        on_success_callback=uploading_logs
    )

    md_account_d = PythonOperator(
        task_id="md_account_d",
        python_callable=insert_data,
        op_kwargs={"table_name" : "md_account_d"},
        on_success_callback=uploading_logs
    )

    md_currency_d = PythonOperator(
        task_id="md_currency_d",
        python_callable=insert_data,
        op_kwargs={"table_name" : "md_currency_d"},
        on_success_callback=uploading_logs
    )

    md_exchange_rate_d = PythonOperator(
        task_id="md_exchange_rate_d",
        python_callable=insert_data,
        op_kwargs={"table_name" : "md_exchange_rate_d"},
        on_success_callback=uploading_logs
    )

    md_ledger_account_s = PythonOperator(
        task_id="md_ledger_account_s",
        python_callable=insert_data,
        op_kwargs={"table_name" : "md_ledger_account_s"},
        on_success_callback=uploading_logs
    )

    split = DummyOperator(
        task_id="split",
        on_success_callback=uploading_logs
    )

    sql_ft_balance_f = SQLExecuteQueryOperator(
        task_id="sql_ft_balance_f",
        conn_id="postgres-db",
        sql="sql/ft_balance_f.sql",
        on_success_callback=uploading_logs
        
    )

    sql_ft_posting_f = SQLExecuteQueryOperator(
        task_id="sql_ft_posting_f",
        conn_id="postgres-db",
        sql="sql/ft_posting_f.sql",
        on_success_callback=uploading_logs
    )

    sql_md_account_d = SQLExecuteQueryOperator(
        task_id="sql_md_account_d",
        conn_id="postgres-db",
        sql="sql/md_account_d.sql",
        on_success_callback=uploading_logs
    )

    sql_md_currency_d = SQLExecuteQueryOperator(
        task_id="sql_md_currency_d",
        conn_id="postgres-db",
        sql="sql/md_currency_d.sql",
        on_success_callback=uploading_logs
    )

    sql_md_exchange_rate_d = SQLExecuteQueryOperator(
        task_id="sql_md_exchange_rate_d",
        conn_id="postgres-db",
        sql="sql/md_exchange_rate_d.sql",
        on_success_callback=uploading_logs
    )

    sql_md_ledger_account_s = SQLExecuteQueryOperator(
        task_id="sql_md_ledger_account_s",
        conn_id="postgres-db",
        sql="sql/md_ledger_account_s.sql",
        on_success_callback=uploading_logs
    )

    create_log_sheme = SQLExecuteQueryOperator(
        task_id='create_log_sheme',
        conn_id='postgres-db',
        sql='sql/create_log_sheme.sql',
        on_success_callback=uploading_logs
    )

    create_schema_ds = SQLExecuteQueryOperator(
        task_id='create_schema_ds',
        conn_id='postgres-db',
        sql='sql/create_schema_ds.sql',
        on_success_callback=uploading_logs
    )

    end = DummyOperator(
        task_id="end",
        on_success_callback=uploading_logs
    )
    {
        start
        >>task_start_log
        >>[create_schema_ds, create_log_sheme, ft_balance_f, ft_posting_f, md_account_d, md_currency_d, md_exchange_rate_d, md_ledger_account_s]
        >>split
        >>[sql_ft_balance_f, sql_ft_posting_f, sql_md_account_d, sql_md_currency_d, sql_md_exchange_rate_d, sql_md_ledger_account_s]
        >>end
    }

from airflow import DAG
import pandas
import codecs
from datetime import datetime
from airflow.configuration import conf
from airflow.models import Variable 
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator 
from airflow.providers.postgres.hooks.postgres import PostgresHook


PATH = Variable.get("my_path")
conf.set("core", "template_searchpath", PATH)

postgres_hook = PostgresHook("dwh_db")
engine = postgres_hook.get_sqlalchemy_engine()
date_now = datetime.now()

def import_(table_name):
    try:
        df = pandas.read_csv(PATH + f"{table_name}.csv", delimiter=",", encoding='utf-8')
    except UnicodeDecodeError:
        df = pandas.read_csv(PATH + f"{table_name}.csv", delimiter=",", encoding='cp1251') 
    df.to_sql(table_name, engine, schema="rd", if_exists="append", index=False) 

default_args = {
    "owner" : "budanovsa",
    "start_date" : date_now,
    "retries" : 2
}
with DAG(
    "import_date_rd",
    default_args=default_args,
    description="Загрузка CSV в базу",
    catchup=False,
    template_searchpath = [PATH],
    # schedule="0 0 * * *"
) as dag:
    start = DummyOperator(
        task_id="start",
    )
    import_deal_info = PythonOperator(
        task_id="import_deal_info",
        python_callable=import_,
        op_kwargs={"table_name" : "deal_info"},
    )
    import_product_info = PythonOperator(
        task_id="import_product_info",
        python_callable=import_,
        op_kwargs={"table_name" : "product_info_csv"},
    )    
    end = DummyOperator(
        task_id="end",
    )
    {
        start
        >>[import_deal_info, import_product_info]
        >>end
    }

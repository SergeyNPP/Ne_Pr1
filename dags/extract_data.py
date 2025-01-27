# ▎ Скрипт для выгрузки данных в CSV

# Вот пример скрипта на Python, который выгружает данные из витрины «dm.dm_f101_round_f» в CSV-файл:

# ```python
from airflow import DAG
import pandas as pd
import sqlalchemy
from datetime import datetime
from airflow.configuration import conf
from airflow.models import Variable 
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator 
from airflow.providers.postgres.hooks.postgres import PostgresHook


postgres_hook = PostgresHook("postgres-db")
engine = postgres_hook.get_sqlalchemy_engine()
# Настройки подключения к базе данных
# def extract_date ():
#     DATABASE_URI = 'postgresql://airflow:airflow@localhost:5432/dm.dm_f101_round_f' 
# # 'ваш_URI_для_подключения'  # Замените на фактический URI вашей базы данных
#     TABLE_NAME = 'dm.dm_f101_round_f'
#     OUTPUT_CSV = 'dm.dm_f101_round_f_v1.csv'

# # Создаем подключение к базе данных
#     engine = sqlalchemy.create_engine(DATABASE_URI)

# # Читаем данные из таблицы
#     data = pd.read_sql_table(TABLE_NAME, engine)

# # Сохраняем данные в CSV-файл
#     data.to_csv(OUTPUT_CSV, index=False, encoding='utf-8-sig')

def extract(**kwargs):
    context = kwargs
    task_instance = context['task_instance'].task_id
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

def uploading_logs(**kwargs):
    context = kwargs
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
        on_success_callback=uploading_logs
    )
    extract_date = PythonOperator(
        task_id="extract_date",
        python_callable=extract,
        on_success_callback=uploading_logs
    )
    end = DummyOperator(
        task_id="end",
        on_success_callback=uploading_logs
    )
    {
        start
        >>[extract_date]
        >>end
    }
# ```

# ▎ Объяснение кода

# 1. Импортируем необходимые библиотеки: `pandas` для работы с данными и `sqlalchemy` для подключения к базе данных.
# 2. Задаем настройки подключения к базе данных, включая `DATABASE_URI` и имя таблицы `TABLE_NAME`.
# 3. Устанавливаем подключение к базе данных с помощью SQLAlchemy.
# 4. Читаем данные из таблицы с помощью `pd.read_sql_table`.
# 5. Сохраняем данные в CSV-файл с помощью `to_csv`, указав `index=False`, чтобы не включать индекс в файл. 

# Убедитесь, что у вас установлены библиотеки `pandas` и `sqlalchemy`, прежде чем запускать скрипт.
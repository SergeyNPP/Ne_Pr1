# ▎ Скрипт для выгрузки данных в CSV

# Вот пример скрипта на Python, который выгружает данные из витрины «dm.dm_f101_round_f» в CSV-файл:

# ```python
from airflow import DAG
import pandas
import sqlalchemy
from datetime import datetime
from airflow.configuration import conf
from airflow.models import Variable 
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator 
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

    # append
    # replace
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

PATH = Variable.get("my_path")
conf.set("core", "template_searchpath", PATH)

postgres_hook = PostgresHook("postgres-db")
engine = postgres_hook.get_sqlalchemy_engine()
date_now = datetime.now()

def import_(table_name):
    try:
        df = pandas.read_csv(PATH + f"{table_name}.csv", delimiter=",", encoding='utf-8')
    except UnicodeDecodeError:
        df = pandas.read_csv(PATH + f"{table_name}.csv", delimiter=",", encoding='Windows-1252')
    df.to_sql(table_name, engine, schema="stage", if_exists="append", index=False) 


def uploading_logs(**kwargs):
    context = kwargs
    task_instance = context['task_instance'].task_id
    ts = context['task_instance'].execution_date.timestamp()
    ts = datetime.fromtimestamp(ts)
    
    tablename = 'dm_f101_round_f'
    log_schema = 'log'
    log_table = 'logt'
    query = f"""
        INSERT INTO {log_schema}.{log_table} (execution_datetime, event_datetime, event_name)
        VALUES ('{ts}', '{datetime.now().isoformat(sep='T')}', 'import_{tablename}');
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
    description="Выгрузка в CSV",
    catchup=False,
    template_searchpath = [PATH],
    schedule="0 0 * * *"
) as dag:
    start = DummyOperator(
        task_id="start",
        on_success_callback=uploading_logs
    )
    import_date = PythonOperator(
        task_id="import_date",
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
        >>[import_date]
        >>split
        >>[sql_dm_f101_round_f]
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
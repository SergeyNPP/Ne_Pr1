




# def extract(tablename):

#     task_instance = context['task_instance'].task_id
#     ts = context['task_instance'].execution_date.timestamp()
#     ts = datetime.fromtimestamp(ts)
#     tablename = 'dm.dm_f101_round_f'
#     log_schema = 'log'
#     log_table = 'logt'
#     query = f"""
#         COPY (SELECT * FROM {tablename}) TO 'D:/DE/VSCODE/AIRFLOW/Airflow/files/sql/dm_f101_round_f.csv' WITH CSV HEADER;
#         INSERT INTO {log_schema}.{log_table} (execution_datetime, event_datetime, event_name)
#         VALUES ('{ts}', '{datetime.now().isoformat(sep='T')}', 'dm_f101_round_f');
#     """
#     postgres_hook.run(query)





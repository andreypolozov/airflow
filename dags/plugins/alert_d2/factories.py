import pendulum
import datetime
import os
from os import PathLike
from typing import Any, Optional, List, Dict, Iterable, Union
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from plugins.alert_d2.operators_test import SQLCheckOperator, GetD2Updates
from airflow.providers.telegram.hooks.telegram import TelegramHook
import yaml
from yaml.loader import SafeLoader
import logging


default_args = {
    'owner':           'BI',
    'retries':         0,
    'retry_delay':     0,
    'catchup':         False
}



class DagFactory():

    __cache = set()
    @classmethod
    def new(cls,
            dag_id: str,
            schema_path: Union[PathLike, str] = None,
            sql_path: Union[PathLike, str] = None,
            src_conn_id: str = 'impala',
            telegram_conn_id: str = 'telegram_alert_test',
            chat_id: str = '-1001755658201',
            schedule_interval: Any = None,
            max_active_runs: int = 1,
            start_date: datetime.datetime = pendulum.today('UTC').add(days=-1),
            mentions: Optional[List[Dict[str, Any]]] = None,
            tags: Iterable[str] = ['alert_d2', 'service_dag'],
            **kwargs):

        if dag_id in cls.__cache:
            raise ValueError(f'Duplicated dag_id: {repr(dag_id)}')



        def send_message_telegram(context):

            chat_id = context['params'].get('chat_id')
            message = f'''⚡️ <strong>{context['params'].get('message')}</strong>
        <strong>Таблица</strong>: {context['params'].get('table')}
        <strong>Метрика</strong>: {context['params'].get('metrics')}'''

            logging.info(f'Sending message to {chat_id}:\n{message}')

            telegram_hook = TelegramHook(telegram_conn_id='telegram_alert_test', chat_id=chat_id)
            telegram_hook.send_message({"text": message, 'parse_mode': 'html'})

        def get_data_from_yaml(schema_path):

            path = os.path.abspath(schema_path)
            logging.info(f'open yaml file link: {path}')

            with open(path) as file:
                data = yaml.load(file, Loader=SafeLoader)

            return data

        def get_update_line(data):

            update_line = {}

            for row in data:
                name_table = row['table']
                update_line[name_table] = row['metrics']

            return update_line

        def get_messages(data):

            messages = {}

            for row in data:
                for key, value in zip(row['metrics'], row['message']):
                    messages[key] = value

            return messages


        dag_args = dict(
            dag_id=dag_id,
            default_args=default_args,
            schedule_interval=schedule_interval,
            start_date=start_date,
            tags=tags,
            max_active_runs=max_active_runs
        )

        logging.info(f'dag_args: {dag_args}')

        alert_dag = DAG(**dag_args)

        with alert_dag:

            data = get_data_from_yaml(schema_path)
            logging.info(f'get data: {data}')

            update_line = get_update_line(data)
            logging.info(f'get update_line: {update_line}')

            messages = get_messages(data)
            logging.info(f'get messages: {messages}')

            groups = []

            for table in update_line:

                tasks = []

                tg_id = f"{table.replace('.', '_')}"
                logging.info(f'Create task group {table}')

                with TaskGroup(group_id=tg_id, dag=alert_dag) as tg1:

                    tasks.append(GetD2Updates(
                        task_id=f'get_d2_updates_from_{table}',
                        postgres_conn_id='d2',
                        params={"chat_id": chat_id, "table": table, "metrics": f'updates_from_{table}', "message": 'Отсутствует обновление'},
                        on_failure_callback=send_message_telegram
                    ))

                    for metrics in update_line[table]:

                        logging.info(f'Create task {metrics}')
                        message = messages[metrics]

                        tasks.append(SQLCheckOperator(
                            task_id=f'{metrics}_task',
                            sql=f'{sql_path}/{metrics}.sql',
                            conn_id='impala',
                            dag=alert_dag,
                            params={"chat_id": chat_id, "table": table, "metrics": metrics, "message": message},
                            on_failure_callback=send_message_telegram
                        ))

                    logging.info(f'Creating dependencies between tasks inside current task group')
                    for i in range(0, len(tasks) - 1):
                        tasks[i] >> tasks[i + 1]

                    groups.append(tg1)

            logging.info(f'Creating dependencies between tasks group')
            for i in range(0, len(groups) - 1):
                groups[i] >> groups[i + 1]

        g = globals()
        cls.__cache.add(dag_id)
        g[dag_id] = alert_dag
        return alert_dag



"""def send_message_telegram(context):

                chat_id = context['params'].get('chat_id')
                message = f'''⚡️ <strong>{context['params'].get('message')}</strong>
<strong>Таблица</strong>: {context['params'].get('table')}
<strong>Метрика</strong>: {context['params'].get('metrics')}'''

                logging.info(f'Sending message to {chat_id}:\n{message}')

                telegram_hook = TelegramHook(telegram_conn_id='telegram_alert_test', chat_id=chat_id)
                telegram_hook.send_message({"text": message, 'parse_mode': 'html'})"""



"""            def get_records(data_id):

                today = arrow.utcnow().floor('day')
                timeframe_start = today.shift(hours=-3).int_timestamp
                timeframe_end = today.shift(hours=+7).int_timestamp
                query = f'''SELECT data_id
                            FROM
                              (SELECT '{data_id}' AS data_id) td
                            LEFT JOIN
                              (SELECT count(data_id) AS num,
                                      data_id
                               FROM updates
                               WHERE data_id = '{data_id}'
                                 AND execution_time >= {timeframe_start}
                                 AND execution_time <= {timeframe_end}
                               GROUP BY data_id) tc USING (data_id)
                            WHERE coalesce(num, 0) = 0;'''

                print(query)
                return sqlparse.format(query, reindent=True, keyword_case='upper', indent_columns=False)

            def check_updates(**context):

                postgres_conn_id = 'd2'
                data_id = 'impala:' + context['params'].get('table')
                hook = PostgresHook(postgres_conn_id=postgres_conn_id)

                sql = get_records(data_id)
                result = [r[0] for r in hook.get_records(sql)]

                print(result)
                if result:
                    raise AirflowFailException"""

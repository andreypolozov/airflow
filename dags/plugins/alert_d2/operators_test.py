from __future__ import annotations
import arrow
import sqlparse
from airflow.providers.common.sql.operators.sql import BaseSQLOperator
from typing import TYPE_CHECKING, Iterable, Mapping,  Sequence, NoReturn
from kg_airflow_providers.hooks.impala import ImpalaHook
from airflow.exceptions import AirflowFailException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
if TYPE_CHECKING:
    from airflow.utils.context import Context

class SQLCheckOperator(BaseSQLOperator):
    """
    Performs checks against a db. The ``SQLCheckOperator`` expects
    a sql query that will return a single row. Each value on that
    first row is evaluated using python ``bool`` casting. If any of the
    values return ``False`` the check is failed and errors out.

    Note that Python bool casting evals the following as ``False``:

    * ``False``
    * ``0``
    * Empty string (``""``)
    * Empty list (``[]``)
    * Empty dictionary or set (``{}``)

    :param sql: the sql to be executed. (templated)
    :param conn_id: the connection ID used to connect to the database.
    :param database: name of database which overwrite the defined one in connection
    :param parameters: (optional) the parameters to render the SQL query with.
    """

    template_fields: Sequence[str] = ("sql",)
    template_ext: Sequence[str] = (
        ".hql",
        ".sql",
    )
    template_fields_renderers = {"sql": "sql"}
    ui_color = "#fff7e6"

    def __init__(
        self,
        *,
        sql: str,
        conn_id: str | None = None,
        database: str | None = None,
        parameters: Iterable | Mapping | None = None,
        **kwargs,
    ) -> None:
        super(SQLCheckOperator, self).__init__(conn_id=conn_id, database=database, **kwargs)
        #super().__init__(conn_id=conn_id, database=database, **kwargs)
        self.sql = sql
        self.parameters = parameters

    def execute(self, context: Context):
        self.log.info("Executing SQL check: %s", self.sql)
        impala_hook = ImpalaHook(conn_id='impala')
        self.log.info("get first records, use mem_limit=1")
        records = impala_hook.get_first(sql=self.sql, mem_limit=1)
        # records = self.get_db_hook().get_first(self.sql, self.parameters)
        # records = impala_hook.get_records(sql=self.sql, mem_limit=1)
        # records = records[0]
        self.log.info("Record: %s", records)

        if not records:
            self._raise_exception(f"The following query returned zero rows: {self.sql}")
        elif not all(bool(r) for r in records.values()):
            self._raise_exception(f"Test failed.\nQuery:\n{self.sql}\nResults:\n{records!s}")

        self.log.info("Success.")



class GetD2Updates(BaseOperator):

    def __init__(
        self,
        *,
        postgres_conn_id: str = 'd2',
        **kwargs,
    ):
        super(GetD2Updates, self).__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id

    def get_records(self, data_id):

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

        self.log.info("Create SQL querry: %s", query)

        return sqlparse.format(query, reindent=True, keyword_case='upper', indent_columns=False)

    def execute(self, context: Context):

        data_id = 'impala:' + context['params'].get('table')
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        self.log.info("Get records from postgres: %s", data_id)
        sql = self.get_records(data_id)

        self.log.info("Answer from postgres: %s", sql)
        result = [r[0] for r in hook.get_records(sql)]

        self.log.info("result: %s", result)

        if result:
            self._raise_exception(f"Test failed.\n Result not empty\n Results:\n{result!s}")

        self.log.info("Success.")

    def _raise_exception(self, exception_string: str) -> NoReturn:

        raise AirflowFailException(exception_string)



"""class MessageTelegramOperator:

    @classmethod
    def run(self, context: Context):

        print('I am in MessageTelegramOperator')
        print(context)

        chat_id = context['params'].get('chat_id')
        message = f'''⚡️ <strong>{context['params'].get('message')}</strong>
        <strong>Таблица</strong>: {context['params'].get('table')}
        <strong>Метрика</strong>: {context['params'].get('metrics')}'''

        logging.info(f'Sending message to {chat_id}:\n{message}')

        telegram_hook = TelegramHook(telegram_conn_id='telegram_alert_test', chat_id=chat_id)
        telegram_hook.send_message({"text": message, 'parse_mode': 'html'})"""


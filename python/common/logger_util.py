import logging
import psycopg2
from psycopg2 import sql
from common.utils import get_conn_params

class PostgresDBHandler(logging.Handler):
    """
    Taken from: https://stackoverflow.com/a/43843623/4282381
    Customized logging handler that puts logs to a Postgres database.
    Requires that target table logs.{function_name}_log table is already available.
    conn_params must include dbname, user, password, host, port.

    Logging library docs: https://docs.python.org/3/library/logging.html
    """

    def __init__(self, function_name: str):
        logging.Handler.__init__(self)
        try:
            self.sql_conn = psycopg2.connect(get_conn_params())
            self.sql_cursor = self.sql_conn.cursor()
        except psycopg2.OperationalError as err:
            self.sql_conn, self.sql_cursor = None, None
            print(f"Could not initialize PostgresDBHandler for logging: {err}")
        self.target_table = f"{function_name}_log"
        self.query_template = sql.SQL(
            "INSERT INTO logs.{table} (log_level, log_text) VALUES (%s, %s)"
        ).format(table=sql.Identifier(self.target_table))

    def emit(self, record):
        log_level = record.levelname.lower()
        log_msg = record.msg.strip()
        try:
            self.sql_cursor.execute(self.query_template, (log_level, log_msg))
            self.sql_conn.commit()
        # If error - print it out on screen. Since DB is not working - there's
        # no point making a log about it to the db.
        except Exception as e:
            print(f"Logging to database failed: {e}")

    def __del__(self):
        """Clean up the db connection when this handler is destroyed.
        In practice, this gets called as a result of logger.removeHandler(handler)."""
        if self.sql_conn:
            self.sql_conn.close()

# get_custom_db_log_handler(function_name='importer', conn_params=get_conn_params())

class CustomDbLogHandler():
    """
    Create an instance of this class once at the start of every function app endpoint
    where you need db logging. After this, you can reference to the logger by calling
    logger = logging.getLogger(<function_name>)

    NOTE: remember to call remove_handlers() in the end of the function app endpoint
    """
    def __init__(self, function_name: str):
        logger = logging.getLogger('importer')
        logger.setLevel('DEBUG')

        logging_formatter = logging.Formatter(
            "%(name)-12s %(asctime)s %(levelname)-8s %(filename)s:%(funcName)s %(message)s")

        console_log_handler = logging.StreamHandler()
        console_log_handler.setFormatter(logging_formatter)
        console_log_handler.addFilter(logging.Filter('importer'))
        logger.addHandler(console_log_handler)

        db_log_handler = PostgresDBHandler(function_name=function_name)
        db_log_handler.setFormatter(logging_formatter)
        db_log_handler.addFilter(logging.Filter('importer'))
        logger.addHandler(db_log_handler)

        self.db_log_handler = db_log_handler
        self.console_log_handler = console_log_handler
        self.logger = logger

    def remove_handlers(self):
        self.logger.removeHandler(self.console_log_handler)
        self.logger.removeHandler(self.db_log_handler)

import logging
import psycopg2


class PostgresDBHandler(logging.Handler):
    """
    Taken from: https://stackoverflow.com/a/43843623/4282381
    Customized logging handler that puts logs to a Postgres database.
    Requires that target table logs.{function_name}_log table is already available.
    conn_params must include dbname, user, password, host, port.

    Logging library docs: https://docs.python.org/3/library/logging.html
    """

    def __init__(self, function_name: str, conn_params: dict):
        logging.Handler.__init__(self)
        try:
            self.sql_conn = psycopg2.connect(**conn_params)
            self.sql_cursor = self.sql_conn.cursor()
        except psycopg2.OperationalError as err:
            self.sql_conn, self.sql_cursor = None, None
            print(f"Could not initialize PostgresDBHandler for logging: {err}")
        self.function_name = function_name

    def emit(self, record):
        log_level = record.levelname.lower()
        # Clear the log message so that they can be inserted into db (escape quotes).
        self.log_msg = record.msg
        self.log_msg = self.log_msg.strip()
        self.log_msg = self.log_msg.replace('\'', '\'\'')

        sql = f"INSERT INTO logs.{self.function_name}_log (log_level, log_text) \
            VALUES ('{log_level}', '{self.log_msg}')"

        try:
            self.sql_cursor.execute(sql)
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

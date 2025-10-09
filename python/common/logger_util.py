import logging
import traceback

import psycopg2
from psycopg2 import sql
import requests

from common.config import ENVIRONMENT, POSTGRES_CONNECTION_STRING, SLACK_WEBHOOK_URL, SLACK_USERS_TO_ALERT

SLACK_LOG_LEVEL = logging.INFO  # Do not log debug to slack
SLACK_ALERT_LEVELS = ["CRITICAL", "ERROR"]  # Tag users in these log levels
SLACK_LOG_LEVEL_EMOJIS = {
    "CRITICAL": ":sos:",
    "ERROR": ":red_circle:",
    "WARNING": ":large_yellow_circle:",
    "INFO": ":large_green_circle:",
}


class SlackLoggingHandler(logging.Handler):
    def __init__(self, level: int = 0) -> None:
        super().__init__(level)
        # Parse users and convert them to slack tag format
        self.alert_list = ", ".join([f"<@{u}>" for u in SLACK_USERS_TO_ALERT])

    def emit(self, record):
        if not SLACK_WEBHOOK_URL:
            # Slack url not configured, exit
            return
        log_level = record.levelname.upper()
        log_msg = record.msg.strip()

        alert = log_level in SLACK_ALERT_LEVELS
        emoji = SLACK_LOG_LEVEL_EMOJIS[log_level]

        msg_object = {
            "text": (
                f"{emoji} Msg from {ENVIRONMENT} [{log_level}]: "
                f"{self.alert_list if alert and self.alert_list and ENVIRONMENT == 'PROD' else ''}\n"
                f"```{log_msg}```"
            )
        }
        requests.post(SLACK_WEBHOOK_URL, json=msg_object)


class PostgresDBHandler(logging.Handler):
    """
    Taken from: https://stackoverflow.com/a/43843623/4282381
    Customized logging handler that puts logs to a Postgres database.
    Requires that target table logs.{function_name}_log table is already available.
    conn_params must include dbname, user, password, host, port.

    Logging library docs: https://docs.python.org/3/library/logging.html
    """

    def __init__(self, function_name: str):
        super().__init__()
        try:
            self.sql_conn = psycopg2.connect(POSTGRES_CONNECTION_STRING)
            self.sql_cursor = self.sql_conn.cursor()
        except psycopg2.OperationalError as err:
            self.sql_conn, self.sql_cursor = None, None
            print(f"Could not initialize PostgresDBHandler for logging: {err}")
        self.target_table = f"{function_name}_log"
        self.query_template = sql.SQL("INSERT INTO logs.{table} (log_level, log_text) VALUES (%s, %s)").format(
            table=sql.Identifier(self.target_table)
        )

    def emit(self, record):
        if not (self.sql_cursor and self.sql_conn):
            # DB logging not correctly initialized, exit.
            return

        log_level = record.levelname.lower()
        log_msg = record.msg.strip()

        # Attach traceback info to error message. Available when called logger.exception
        if record.exc_info:
            log_msg += f"\n{traceback.format_exc()}"
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


# Store handlers here to prevent doubled instances
log_handler_store = {}


class CustomDbLogHandler:
    """
    Use this context manager once at the start of every function app endpoint
    where you need db logging. After this, you can reference to the logger by calling
    logger = logging.getLogger(<function_name>)
    """

    def __init__(self, function_name: str):
        self.logger_name = function_name
        self.logger = logging.getLogger(function_name)

        if self.logger_name not in log_handler_store:
            # Logger not initialized before, create handlers
            self.logger.setLevel("DEBUG")
            logging_formatter = logging.Formatter(
                "%(name)-12s %(asctime)s %(levelname)-8s %(filename)s:%(funcName)s %(message)s"
            )

            log_handlers = [
                logging.StreamHandler(),
                PostgresDBHandler(function_name=function_name),
                SlackLoggingHandler(level=SLACK_LOG_LEVEL),
            ]

            for handler in log_handlers:
                handler.setFormatter(logging_formatter)
                handler.addFilter(logging.Filter(function_name))
                self.logger.addHandler(handler)

            log_handler_store[function_name] = {"handlers": log_handlers, "count": 1}
        else:
            # Logger already initialized, just increase count
            log_handler_store[function_name]["count"] += 1

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if self.logger_name not in log_handler_store:
            # No handler found, exiting
            return

        # Execution done, decrease count
        log_handler_store[self.logger_name]["count"] -= 1

        if log_handler_store[self.logger_name]["count"] == 0:
            # If no one is using logger, remove handlers.
            for handler in log_handler_store[self.logger_name]["handlers"]:
                self.logger.removeHandler(handler)
                handler.close()
            del log_handler_store[self.logger_name]

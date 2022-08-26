import logging
from sys import stdout
import psycopg2 as psycopg
from common.utils import get_conn_params

importer_log_table = 'logs.importer_log'

class LogDBHandler(logging.Handler):
    """"
    Taken from https://stackoverflow.com/a/43843623/4282381
    Customized logging handler that puts logs to the database.
    """
    def __init__(self, sql_conn, sql_cursor, function_name):
        logging.Handler.__init__(self)
        self.sql_cursor = sql_cursor
        self.sql_conn = sql_conn
        self.function_name = function_name

    def emit(self, record):
        log_level = record.levelname.lower()
        # Clear the log message so it can be put to db via sql (escape quotes)
        self.log_msg = record.msg
        self.log_msg = self.log_msg.strip()
        self.log_msg = self.log_msg.replace('\'', '\'\'')

        sql = f"INSERT INTO logs.{self.function_name}_log (log_level, log_text) \
         VALUES ('{log_level}', '{self.log_msg}')"

        try:
            self.sql_cursor.execute(sql)
            self.sql_conn.commit()
        # If error - print it out on screen. Since DB is not working - there's
        # no point making a log about it to the database.
        except Exception as e:
            print(f"Logging to database failed: {e}")

logger = logging.getLogger('logger')
logger.setLevel(logging.INFO)
logger.setLevel(logging.WARNING)
logger.setLevel(logging.ERROR)
logger.setLevel(logging.DEBUG)

logFormatter = logging.Formatter \
    ("%(name)-12s %(asctime)s %(levelname)-8s %(filename)s:%(funcName)s %(message)s")
consoleHandler = logging.StreamHandler(stdout)  # set streamhandler to stdout
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)

is_db_logger_initialized = False
global log_conn
# Call this at the start of the function
def init_logger(function_name):
    global is_db_logger_initialized
    if not function_name:
        print("init_logger(): function_name has to be given.")
    # Make the connection to database for the logger
    log_conn = psycopg.connect(**get_conn_params())
    log_cursor = log_conn.cursor()
    log_db_handler = LogDBHandler(log_conn, log_cursor, function_name)
    logging.getLogger('logger').addHandler(log_db_handler)
    is_db_logger_initialized = True

def get_logger():
    if is_db_logger_initialized == False:
        print("get_logger() error: did you forget to call init_logger()?")
    return logger

# Remember to call this function in the end of Azure Function.
def close_logger_conn():
    log_conn.close()


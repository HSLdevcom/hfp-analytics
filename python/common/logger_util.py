import logging
from sys import stdout

class Logger(object):
    """
        Using Context Manager: https://book.pythontips.com/en/latest/context_managers.html
    """
    class LogDBHandler(logging.Handler):
        """
        Taken from: https://stackoverflow.com/a/43843623/4282381
        Customized logging handler that puts logs to the database.

        Logging library docs: https://docs.python.org/3/library/logging.html
        """
        def __init__(self, sql_conn, function_name):
            logging.Handler.__init__(self)
            self.sql_cursor = sql_conn.cursor()
            self.sql_conn = sql_conn
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

    def __init__(self, log_conn, function_name):
        logger = logging.getLogger('logger')

        # Sets the threshold for this logger.
        # Those logging messages will be ignored that are less severe
        # than the log level set by setLevel().
        logger.setLevel(logging.DEBUG)

        log_formatter = logging.Formatter \
            ("%(name)-12s %(asctime)s %(levelname)-8s %(filename)s:%(funcName)s %(message)s")
        console_handler = logging.StreamHandler(stdout)  # set streamhandler to stdout
        console_handler.setFormatter(log_formatter)
        logger.addHandler(console_handler)

        log_db_handler = self.LogDBHandler(log_conn, function_name)
        logger.addHandler(log_db_handler)

        self.console_handler = console_handler
        self.logger = logger
        self.log_db_handler = log_db_handler
        self.log_conn = log_conn

    def __enter__(self):
        return self.logger

    def __exit__(self, type, value, traceback):
        self.logger.removeHandler(self.log_db_handler)
        self.logger.removeHandler(self.console_handler)

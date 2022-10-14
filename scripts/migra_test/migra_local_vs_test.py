import sys
from migra import Migration
from sqlbag import S
import psycopg2 as psycopg
import os
from dotenv import load_dotenv

load_dotenv()

LOCAL_DB_CONNECTION_STRING = "postgresql://postgres:postgres@localhost/analytics"
DEPLOY_FILE_NAME = "test_deploy.sql"

def is_test_db_connection_string_ok(test_db_connection_string):
    if test_db_connection_string is None:
        print("TEST_DB_CONNECTION_STRING was not found, do you have it defined in .env file?")
        return False
    if len(str(test_db_connection_string)) < 10:
        print("TEST_DB_CONNECTION_STRING length is invalid")
        return False
    return True

def main():
    """
        Compares current local and test database schemas,
        makes test database schema the same as local database schema.

        Requirements: have .env file in this current folder with test_DB_CONNECTION_STRING.
        That connection string has the same format as LOCAL_DB_CONNECTION_STRING.

        Before doing anything, make sure that your local database is up-to-date
        with the current schema definition (in db/sql). After that, you can run
        the script with "init" param and then go through migra_test.sql.

        If everything looks good, you can run the script with apply argument.
    """

    test_db_connection_string = os.getenv('TEST_DB_CONNECTION_STRING')
    if is_test_db_connection_string_ok(test_db_connection_string) == False:
        return

    args = list(sys.argv)
    if len(args) > 1:
        arg = args[1]
        if arg == "print" or arg == "init":
            with S(LOCAL_DB_CONNECTION_STRING) as s_local, S(test_db_connection_string) as s_test:
                m = Migration(x_from=s_test, x_target=s_local, exclude_schema='_timescaledb_internal')
                m.set_safety(False)
                m.add_all_changes()
                if m.statements:
                    if arg == "print":
                        print(m.sql)
                    if arg == "init":
                        print("Creating test_deploy.sql file...")
                        f = open(DEPLOY_FILE_NAME, "w")
                        f.write(m.sql)
                        f.close()
                else:
                    print("No pending changes were found.")
            return
        if arg == "apply":
            print(f"Do you want to apply changes to test from >>> {DEPLOY_FILE_NAME} <<< file?")
            confirm_response = input("(type 'yes' to proceed): ")

            if confirm_response == "yes":
                print("Proceeding with changes.")
                apply_changes(test_db_connection_string)
                return
            else:
                print("Applying aborted.")
                return

        print("Invalid args. Supported args are: 'print', 'init' and 'apply'")
    else:
        print("Invalid args. Supported args are: 'print', 'init' and 'apply'")

def apply_changes(test_db_connection_string):
    try:
        f = open(DEPLOY_FILE_NAME, "r")
        sql_content = f.read()
        print(sql_content)
        f.close()

        conn = psycopg.connect(test_db_connection_string)
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute(sql_content)
        finally:
            print("Apply done.")
            conn.close()

    except OSError:
        print(f"Could not open/read file: {DEPLOY_FILE_NAME}")
        print("Did you forget to run the script with 'init' argument?")

if __name__ == '__main__':
    main()
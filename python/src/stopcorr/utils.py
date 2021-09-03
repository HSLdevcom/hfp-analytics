import os

def get_conn_params():
    return dict(
        dbname = os.getenv('POSTGRES_DB'),
        user = os.getenv('POSTGRES_USER'),
        password = os.getenv('POSTGRES_PASSWORD'),
        host = 'db',
        port = 5432
    )

def env_with_default(var_name, default_value):
    res = os.getenv(var_name)
    if res is None:
        res = default_value
        print(f'{var_name} not set, falling back to default value {default_value}')
    return res

def comma_separated_floats_to_list(val_str):
    res = val_str.split(',')
    res = [x.strip() for x in res]
    res = [float(x) for x in res]
    return res

def comma_separated_integers_to_list(val_str):
    res = val_str.split(',')
    res = [x.strip() for x in res]
    res = [int(x) for x in res]
    return res

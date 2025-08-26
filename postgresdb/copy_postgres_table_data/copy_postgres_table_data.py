import psycopg2
import configparser
import sys

def load_db_config(env, config_file='db_config.ini'):
    config = configparser.ConfigParser()
    config.read(config_file)
    if env not in config:
        raise ValueError(f"Config for environment '{env}' not found.")
    cfg = config[env]
    required_keys = ['host', 'database', 'user', 'password', 'table']
    for key in required_keys:
        if key not in cfg:
            raise ValueError(f"Missing '{key}' in '{env}' config.")
    return cfg

def get_connection(cfg):
    try:
        conn = psycopg2.connect(
            host=cfg['host'],
            database=cfg['database'],
            user=cfg['user'],
            password=cfg['password'],
            sslmode='require'
        )
        return conn
    except Exception as e:
        raise ConnectionError(f"Failed to connect to {cfg['database']} at {cfg['host']}: {e}")

def fetch_table_data(conn, table):
    with conn.cursor() as cur:
        try:
            cur.execute(f"SELECT * FROM {table}")
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]
            return colnames, rows
        except Exception as e:
            raise RuntimeError(f"Error fetching data from {table}: {e}")

def insert_table_data(conn, table, colnames, rows):
    if not rows:
        print(f"No data to insert into {table}.")
        return 0
    with conn.cursor() as cur:
        col_list = ','.join([f'"{c}"' for c in colnames])
        placeholders = ','.join(['%s'] * len(colnames))
        insert_sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"
        try:
            cur.executemany(insert_sql, rows)
            conn.commit()
            return len(rows)
        except Exception as e:
            conn.rollback()
            raise RuntimeError(f"Error inserting data into {table}: {e}")

def validate_schemas(src_cols, dest_cols):
    if src_cols != dest_cols:
        raise ValueError(f"Source and destination table schemas do not match.\nSource: {src_cols}\nDestination: {dest_cols}")

def main():
    if len(sys.argv) != 3:
        print("Usage: python copy_table_data.py <source_env> <target_env>")
        print("Example: python copy_table_data.py qa stg")
        sys.exit(1)

    source_env = sys.argv[1].lower()
    target_env = sys.argv[2].lower()

    try:
        src_cfg = load_db_config(source_env)
        tgt_cfg = load_db_config(target_env)
        if src_cfg['table'] != tgt_cfg['table']:
            raise ValueError("Table names must match in source and target configs.")

        print(f"Connecting to source: {source_env}")
        src_conn = get_connection(src_cfg)
        print(f"Connecting to target: {target_env}")
        tgt_conn = get_connection(tgt_cfg)

        print(f"Fetching data from {src_cfg['table']} in {source_env}...")
        src_cols, src_rows = fetch_table_data(src_conn, src_cfg['table'])
        print(f"Fetched {len(src_rows)} rows.")

        print(f"Validating destination table schema in {target_env}...")
        tgt_cols, _ = fetch_table_data(tgt_conn, tgt_cfg['table'])
        validate_schemas(src_cols, tgt_cols)
        print("Schema validation successful.")

        print(f"Inserting data into {tgt_cfg['table']} in {target_env}...")
        inserted = insert_table_data(tgt_conn, tgt_cfg['table'], tgt_cols, src_rows)
        print(f"Successfully inserted {inserted} rows into {target_env}.")

    except Exception as ex:
        print(f"ERROR: {ex}")
        sys.exit(2)
    finally:
        try:
            src_conn.close()
        except:
            pass
        try:
            tgt_conn.close()
        except:
            pass

if __name__ == "__main__":
    main()

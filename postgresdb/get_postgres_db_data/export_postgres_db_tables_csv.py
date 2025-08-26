import os
import csv
import psycopg2
from configparser import ConfigParser

CONFIG_FILE = "config.properties"
CSV_DIR = r"C:\temp\ADO\postgresdb"
CSV_FILE = "db_tables.csv"

def read_db_config(filename=CONFIG_FILE, section='postgresql'):
    """Read database configuration from properties file."""
    parser = ConfigParser()
    if not os.path.exists(filename):
        raise FileNotFoundError(f"Config file '{filename}' not found.")
    parser.read(filename)
    db_config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db_config[param[0]] = param[1]
    else:
        raise Exception(f"Section '{section}' not found in the '{filename}' file.")
    # Validate essential keys
    for key in ['host', 'port', 'user', 'password']:
        if key not in db_config or not db_config[key]:
            raise Exception(f"Missing required config key: '{key}' in '{filename}'.")
    return db_config

def get_databases(conn):
    """Fetch all database names, ignoring templates."""
    with conn.cursor() as cur:
        cur.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
        dbs = [row[0] for row in cur.fetchall()]
    return dbs

def get_tables_for_db(db_config, dbname):
    """Fetch all table names for a database."""
    db_config = db_config.copy()
    db_config['dbname'] = dbname
    conn = None
    tables = []
    try:
        conn = psycopg2.connect(**db_config)
        with conn.cursor() as cur:
            cur.execute(
                """SELECT table_schema, table_name 
                   FROM information_schema.tables 
                   WHERE table_type='BASE TABLE' AND table_schema NOT IN ('pg_catalog', 'information_schema')
                   ORDER BY table_schema, table_name;"""
            )
            tables = [(row[0], row[1]) for row in cur.fetchall()]
    except Exception as e:
        print(f"Warning: Could not fetch tables for DB '{dbname}': {e}")
    finally:
        if conn:
            conn.close()
    return tables

def ensure_dir(path):
    """Ensure directory exists."""
    os.makedirs(path, exist_ok=True)

def main():
    try:
        db_config = read_db_config()
    except Exception as e:
        print(f"[Error] {e}")
        return

    # Extract and remove default_db from config to avoid passing it to psycopg2.connect
    default_db = db_config.pop('default_db', 'postgres')

    db_config_for_server = db_config.copy()
    db_config_for_server['dbname'] = default_db

    try:
        conn = psycopg2.connect(**db_config_for_server)
    except Exception as e:
        print(f"[Error] Unable to connect to PostgreSQL server: {e}")
        return

    try:
        db_names = get_databases(conn)
        if not db_names:
            print("[Info] No databases found.")
            return
        csv_rows = []
        for dbname in db_names:
            tables = get_tables_for_db(db_config, dbname)
            if not tables:
                csv_rows.append([dbname, "(no tables)", ""])
            else:
                for schema, table in tables:
                    csv_rows.append([dbname, schema, table])

        ensure_dir(CSV_DIR)
        csv_path = os.path.join(CSV_DIR, CSV_FILE)
        with open(csv_path, mode='w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Database', 'Schema', 'Table'])
            writer.writerows(csv_rows)
        print(f"[Success] Database and tables written to {csv_path}")

    except Exception as e:
        print(f"[Error] {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
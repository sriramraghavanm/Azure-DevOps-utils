import os
import psycopg2
from configparser import ConfigParser

CONFIG_FILE = "config.properties"

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
        with conn.cursor() as cur:
            cur.execute(
                "SELECT datname FROM pg_database WHERE datistemplate = false AND datname LIKE %s ORDER BY datname;",
                ('commercetools_etl_%',)
            )
            dbs = [row[0] for row in cur.fetchall()]
            if dbs:
                print("Databases starting with 'commercetools_etl_':")
                for db in dbs:
                    print(db)
            else:
                print("No databases found starting with 'commercetools_etl_'.")
    except Exception as e:
        print(f"[Error] {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()

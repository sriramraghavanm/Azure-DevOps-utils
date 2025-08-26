import sys
import logging
import yaml
import psycopg2
import pandas as pd
from datetime import datetime
from email.message import EmailMessage
import smtplib
import argparse

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

# --- Config Loader ---
def load_config(config_path="config.yaml"):
    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
        return config
    except Exception as e:
        logging.error(f"Error loading config file: {e}")
        sys.exit(1)

# --- DB Connection ---
def get_db_connection(db_conf):
    try:
        conn = psycopg2.connect(
            host=db_conf["DATABASE_HOST"],
            database=db_conf["DATABASE_NAME"],
            user=db_conf["DATABASE_USER_NAME"],
            password=db_conf["DATABASE_PASSWORD"],
            port=db_conf["DATABASE_PORT"]
        )
        logging.info("Database connection successful.")
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to database: {e}")
        sys.exit(1)

# --- Query Executor ---
def run_query(conn, query):
    try:
        df = pd.read_sql_query(query, conn)
        logging.info(f"Query executed successfully. Rows fetched: {len(df)}")
        return df
    except Exception as e:
        logging.error(f"Failed to execute query: {e}")
        sys.exit(1)

# --- CSV Saver ---
def save_to_csv(df, prefix="nrs_reservations"):
    today = datetime.now().strftime("%Y-%m-%d")
    filename = f"{prefix}_{today}.csv"
    df.to_csv(filename, index=False)
    logging.info(f"Results saved to {filename}")
    return filename

# --- Email Sender ---
def send_email(email_conf, attachment_path):
    sender = email_conf.get("sender")
    recipients = email_conf.get("recipients", [])
    smtp_host = email_conf.get("smtp_host")
    smtp_port = int(email_conf.get("smtp_port", 465))
    smtp_user = email_conf.get("smtp_user", "")
    smtp_password = email_conf.get("smtp_password", "")

    today = datetime.now().strftime("%d %B %Y")
    subject = f"NRS reservations report for {today}"
    body = (
        "Hi,\n"
        "Please find reservation report attached. This email box is not monitored.\n"
        "To update the pipeline setup like adding new user, update the timeline or to add new columns, "
        "Please reach out to CWASupport@brand.com & retailclouddevops@brand.com.\n\n"
        "Thank You!"
    )

    # --- Validations ---
    if not sender or "@" not in sender:
        logging.error("Valid sender email not provided in config.")
        sys.exit(1)
    if not recipients or not all("@" in r for r in recipients):
        logging.error("Valid recipient emails not provided in config.")
        sys.exit(1)
    for var in ["smtp_host", "smtp_port"]:
        if not email_conf.get(var):
            logging.error(f"Missing email config value: {var}")
            sys.exit(1)

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg.set_content(body)

    # Attach CSV
    with open(attachment_path, "rb") as f:
        file_data = f.read()
        msg.add_attachment(file_data, maintype="text", subtype="csv", filename=attachment_path)

    try:
        if smtp_port == 465:
            server = smtplib.SMTP_SSL(smtp_host, smtp_port)
        else:
            server = smtplib.SMTP(smtp_host, smtp_port)
            server.ehlo()
            server.starttls()
        if smtp_user and smtp_password:
            server.login(smtp_user, smtp_password)
        server.send_message(msg)
        server.quit()
        logging.info("Email sent successfully.")
    except Exception as e:
        logging.error(f"Failed to send email: {e}")
        sys.exit(1)

# --- Main ---
def main():
    parser = argparse.ArgumentParser(description="Run NRS reservations report and email results.")
    parser.add_argument("--interval", type=int, default=180, help="Number of days for reservation interval (default: 180)")
    parser.add_argument("--config", type=str, default="config.yaml", help="Path to config file")
    args = parser.parse_args()

    config = load_config(args.config)
    query_template = config.get("query")
    email_conf = config.get("email", {})

    db_conf = {
        "DATABASE_HOST": config.get("DATABASE_HOST"),
        "DATABASE_NAME": config.get("DATABASE_NAME"),
        "DATABASE_USER_NAME": config.get("DATABASE_USER_NAME"),
        "DATABASE_PASSWORD": config.get("DATABASE_PASSWORD"),
        "DATABASE_PORT": config.get("DATABASE_PORT")
    }

    # Validate DB config values
    for key in db_conf:
        if not db_conf[key]:
            logging.error(f"Missing database config value: {key}")
            sys.exit(1)

    if not query_template:
        logging.error("No query found in config.")
        sys.exit(1)
    if not email_conf:
        logging.error("No email config found in config.")
        sys.exit(1)

    # Replace interval placeholder in query
    # Expecting the query to use {interval} as a placeholder
    query = query_template.replace("{interval}", str(args.interval))

    conn = get_db_connection(db_conf)
    df = run_query(conn, query)
    filename = save_to_csv(df)
    conn.close()
    send_email(email_conf, filename)

if __name__ == "__main__":
    main()
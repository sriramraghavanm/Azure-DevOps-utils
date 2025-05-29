import os
import sys
import datetime
import logging
import re
from azure.storage.blob import BlobServiceClient
from configparser import ConfigParser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s'
)

def read_config(config_path="config.properties"):
    """Read configuration from config.properties file."""
    if not os.path.isfile(config_path):
        logging.error(f"Config file {config_path} not found.")
        sys.exit(1)
    config = ConfigParser()
    config.read(config_path)
    if 'azure' not in config:
        logging.error("Missing 'azure' section in config file.")
        sys.exit(1)
    required_keys = ['storage_endpoint', 'container_name', 'storage_account_name', 'storage_account_key']
    for key in required_keys:
        if key not in config['azure']:
            logging.error(f"Missing required config key: {key}")
            sys.exit(1)
    return config['azure']

def sanitize_filename(filename):
    """Replace unsupported Windows filename characters."""
    return re.sub(r'[<>:"/\\|?*]', '_', filename)

def sanitize_path(blob_name):
    """Sanitize each part of the blob 'path' for Windows filesystem."""
    parts = blob_name.split('/')
    sanitized_parts = [sanitize_filename(part) for part in parts]
    return os.path.join(*sanitized_parts)

def create_backup_dir(container_name):
    """Create backup directory if it does not exist."""
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    backup_path = os.path.join("C:\\temp\\ADO\\blob", f"{container_name}-{today}")
    try:
        os.makedirs(backup_path, exist_ok=True)
    except Exception as e:
        logging.error(f"Failed to create backup directory: {e}")
        sys.exit(1)
    return backup_path

def backup_blob_container(storage_endpoint, container_name, storage_account_name, storage_account_key, backup_path):
    """Backup all blobs from the container to backup_path."""
    try:
        blob_service_client = BlobServiceClient(
            account_url=storage_endpoint,
            credential=storage_account_key
        )
        container_client = blob_service_client.get_container_client(container_name)
    except Exception as e:
        logging.error(f"Failed to connect to Azure Blob Storage: {e}")
        sys.exit(1)

    try:
        blobs = container_client.list_blobs()
    except Exception as e:
        logging.error(f"Failed to list blobs: {e}")
        sys.exit(1)

    blob_count = 0
    for blob in blobs:
        blob_name = blob.name
        sanitized_blob_path = sanitize_path(blob_name)
        dest_file_path = os.path.join(backup_path, sanitized_blob_path)
        dest_dir = os.path.dirname(dest_file_path)
        if not os.path.exists(dest_dir):
            os.makedirs(dest_dir, exist_ok=True)
        try:
            with open(dest_file_path, "wb") as file:
                stream = container_client.download_blob(blob)
                file.write(stream.readall())
            logging.info(f"Downloaded blob: {blob_name} -> {dest_file_path}")
            blob_count += 1
        except Exception as e:
            logging.error(f"Failed to download blob {blob_name}: {e}")
    logging.info(f"Backup complete. {blob_count} blobs downloaded.")

def main():
    config = read_config("config.properties")
    storage_endpoint = config['storage_endpoint']
    container_name = config['container_name']
    storage_account_name = config['storage_account_name']
    storage_account_key = config['storage_account_key']
    backup_path = create_backup_dir(container_name)
    backup_blob_container(storage_endpoint, container_name, storage_account_name, storage_account_key, backup_path)

if __name__ == "__main__":
    main()
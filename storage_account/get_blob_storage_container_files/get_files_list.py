import os
import csv
import sys
from datetime import datetime
from azure.storage.blob import ContainerClient
import configparser
from collections import defaultdict

def load_config(config_path: str) -> dict:
    config = configparser.ConfigParser()
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    config.read(config_path)
    if 'AzureBlob' not in config:
        raise KeyError("Missing 'AzureBlob' section in config file.")
    required_keys = ['connection_string', 'container_name']
    for key in required_keys:
        if key not in config['AzureBlob']:
            raise KeyError(f"Missing '{key}' in config file.")
    return config['AzureBlob']

def ensure_dir_exists(directory: str):
    if not os.path.exists(directory):
        os.makedirs(directory)

def get_blob_list(container_client: ContainerClient):
    blob_list = []
    for blob in container_client.list_blobs():
        props = container_client.get_blob_client(blob).get_blob_properties()
        metadata = props.metadata if props.metadata else {}
        blob_info = {
            'file_name': blob.name,
            'file_size_mb': round(props.size / (1024 * 1024), 2),
            'created_on': props.creation_time.strftime('%Y-%m-%d %H:%M:%S') if props.creation_time else '',
            'created_on_dt': props.creation_time if props.creation_time else None,
            'last_modified': props.last_modified.strftime('%Y-%m-%d %H:%M:%S') if props.last_modified else '',
            'last_modified_dt': props.last_modified if props.last_modified else None,
            'content_type': props.content_settings.content_type,
            'metadata': metadata
        }
        blob_list.append(blob_info)
    return blob_list

def sort_blobs_by_dir_and_modified(blob_list):
    dir_groups = defaultdict(list)
    for blob in blob_list:
        dir_path = os.path.dirname(blob['file_name'])
        dir_groups[dir_path].append(blob)
    sorted_blobs = []
    for dir_path in dir_groups:
        blobs_sorted = sorted(
            dir_groups[dir_path],
            key=lambda b: (b['last_modified_dt'] if b['last_modified_dt'] else datetime.min),
            reverse=True
        )
        sorted_blobs.extend(blobs_sorted)
    return sorted_blobs

def write_csv(blob_list, csv_file_path):
    fieldnames = ['file_name', 'file_size_mb', 'created_on', 'last_modified', 'content_type', 'metadata']
    with open(csv_file_path, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for blob in blob_list:
            row = {
                'file_name': blob['file_name'],
                'file_size_mb': blob['file_size_mb'],
                'created_on': blob['created_on'],
                'last_modified': blob['last_modified'],
                'content_type': blob['content_type'],
                'metadata': "; ".join(f"{k}={v}" for k, v in blob['metadata'].items())
            }
            writer.writerow(row)

def main():
    try:
        config_path = 'config.properties'
        config = load_config(config_path)
        output_dir = r'C:\temp\ADO\blob'
        ensure_dir_exists(output_dir)
        now_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        container_name = config['container_name']
        csv_file_path = os.path.join(
            output_dir,
            f"{container_name}_{now_str}.csv"
        )

        container_client = ContainerClient.from_connection_string(
            conn_str=config['connection_string'],
            container_name=container_name
        )

        print("Fetching blob list...")
        blob_list = get_blob_list(container_client)
        if not blob_list:
            print("No blobs found in the container.")
            return

        print(f"Found {len(blob_list)} blobs. Sorting and writing to CSV...")
        blob_list = sort_blobs_by_dir_and_modified(blob_list)

        write_csv(blob_list, csv_file_path)
        print(f"CSV file created at: {csv_file_path}")

    except Exception as ex:
        print(f"Error: {ex}")
        sys.exit(1)

if __name__ == "__main__":
    main()
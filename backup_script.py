import os
import boto3
import time
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

SOURCE_DIR = "/Users/aniketdubey/Desktop/Test"
DESTINATION_BUCKET = "uploadbucket122"
LOG_FILE = "/Users/aniketdubey/Desktop/Accuknox/2 Automated Backup Solution/log_file.txt"

logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

AWS_ACCESS_KEY_ID = "AKI##############"
AWS_SECRET_ACCESS_KEY = "0LKG6UR1LavoVoXvd############"
AWS_REGION = "us-east-2"

s3 = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_ACCESS_KEY, region_name=AWS_REGION)

def upload_to_s3(local_file, s3_key):
    try:
        s3.upload_file(local_file, DESTINATION_BUCKET, s3_key)
        return True
    except Exception as e:
        logging.error(f"Error uploading {local_file} to S3: {str(e)}")
        return False

def perform_backup():
    timestamp = time.strftime("%Y%m%d%H%M%S")
    backup_folder = f"backup_{timestamp}"

    os.makedirs(backup_folder)

    for root, dirs, files in os.walk(SOURCE_DIR):
        for file in files:
            local_path = os.path.join(root, file)
            s3_key = os.path.relpath(local_path, SOURCE_DIR)
            s3_key = os.path.join(backup_folder, s3_key)

            if upload_to_s3(local_path, s3_key):
                logging.info(f"Uploaded {local_path} to {s3_key}")
            else:
                logging.error(f"Failed to upload {local_path} to S3")


class BackupEventHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        logging.info(f"New file created: {event.src_path}")
        perform_backup()

def main():
    logging.info("Backup script started.")

    event_handler = BackupEventHandler()
    observer = Observer()
    observer.schedule(event_handler, path=SOURCE_DIR, recursive=True)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

    logging.info("Backup script stopped.")

if __name__ == "__main__":
    main()

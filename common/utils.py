import argparse
from google.cloud import storage
from common.Logger import Logger

logger = Logger().get_logger()


def download_file(gcs_path, tgt_path):
    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_path.split("/")[2])
    blob = bucket.blob("/".join(gcs_path.split("/")[3:]))
    blob.download_to_filename(tgt_path)
    logger.info(f"Downloaded config to {tgt_path}")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--runner', required=False, default='DirectRunner', dest='runner')
    parser.add_argument('--config_path', required=True, default='', dest='config_path')
    return parser


def batch_elements(elements, batch_size=1000):
    batch = []
    for element in elements:
        batch.append(element)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch

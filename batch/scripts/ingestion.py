from batch.batchConfig.config import config
from minio import Minio
from minio.error import MinioException
import os
import logging

logger = logging.getLogger(__name__)

def ingestion():
    try:
        logger.info("Starting ingestion process...")

        minio_client = Minio("minio:9000",access_key=config.access_key, secret_key=config.secret_key,secure=False )



        if not minio_client.bucket_exists(config.bucket_name):
            minio_client.make_bucket(config.bucket_name)
            logger.info(f"Bucket '{config.bucket_name}' created successfully.")
        else:
            logger.info(f"Bucket '{config.bucket_name}' already exists.")

        if not os.path.exists(config.landing_path):
            raise FileNotFoundError(f"Landing folder not found at {config.landing_path}")

        for file_name in os.listdir(config.landing_path):
            file_path = os.path.join(config.landing_path, file_name)
            if os.path.isfile(file_path):
                object_name = f"{config.bronze_prefix}{file_name}"
                minio_client.fput_object(config.bucket_name, object_name, file_path)
                logger.info(f"Uploaded {file_name} → {config.bucket_name}/{object_name}")

        logger.info("Ingestion completed successfully.")

    except MinioException as me:
        logger.exception(f"MinIO error: {str(me)}")
        raise
    except Exception as e:
        logger.exception(f"Unexpected error: {str(e)}")
        raise

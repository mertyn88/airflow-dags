from amplitude import properties
from datetime import date, timedelta, datetime
import os
import glob
import boto3
import logging

logger = logging.getLogger()


class Model:
    def __init__(self, dt):
        self.__base_path = f'{properties.base_path}/amplitude'
        if dt is None:
            self.__date = date.today() - timedelta(1)
        else:
            self.__date = datetime.strptime(dt, '%Y%m%d').date()

    @property
    def get_normal_key(self):
        return properties.s3_normal_key

    @property
    def get_secret_key(self):
        return properties.s3_secret_key

    @property
    def get_bucket(self):
        return properties.s3_bucket

    @property
    def get_prefix(self):
        return (properties.s3_prefix
                .replace('{year}', f'{self.__date:%Y}')
                .replace('{month}', f'{self.__date:%m}')
                .replace('{day}', f'{self.__date:%d}')
                )

    @property
    def get_region(self):
        return properties.s3_region

    @property
    def get_files(self):
        return glob.glob(f'{self.__base_path}/result/**', recursive=False)

    @property
    def get_file_name(self):
        return f'{self.__date:%d}.ndjson'


def __get_s3_client(model):
    return boto3.client('s3',
                        aws_access_key_id=model.get_normal_key,
                        aws_secret_access_key=model.get_secret_key,
                        region_name=model.get_region
                        )


def run(dt=None):
    model = Model(dt)
    s3_client = __get_s3_client(model)
    for file in model.get_files:
        upload_path = model.get_prefix.replace('{file}', os.path.basename(file))
        s3_client.upload_file(file, model.get_bucket, upload_path)
        logger.info(upload_path)

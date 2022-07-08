from amplitude import properties
import json
import os
import glob
import gzip
import logging

logger = logging.getLogger()


class Model:
    def __init__(self):
        self.__base_path = f'{properties.base_path}/amplitude'

    @property
    def get_files(self):
        return list(filter(lambda x: x.endswith('json.gz'), glob.glob(f'{self.__base_path}/export/**', recursive=True)))

    @property
    def get_base_path(self):
        return self.__base_path


def __get_gzip_data(path):
    with gzip.open(path, 'rb') as f:
        return f.readlines()


def __get_open(path):
    if os.path.exists(path):
        return open(path, 'a')
    return open(path, 'w')


def __write(file, data):
    file.write(data)
    file.close()


def __line_by_write(zip_path, base_path):
    logger.info(zip_path)
    for line in __get_gzip_data(zip_path):
        try:
            __write(__get_open(f"{base_path}/result/{json.loads(line)['event_type']}"), line.decode('unicode_escape'))
        except Exception:
            logger.error(f'Parse error :: {line}')
            pass


def run():
    model = Model()
    for zip_path in model.get_files:
        __line_by_write(zip_path, model.get_base_path)

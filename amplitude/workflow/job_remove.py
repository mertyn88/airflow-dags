from amplitude import properties
import shutil
import os

import logging

logger = logging.getLogger()


class Model:
    def __init__(self):
        self.__base_path = f'{properties.base_path}/amplitude'

    @property
    def get_result_path(self):
        return f'{self.__base_path}/result'

    @property
    def get_export_path(self):
        return f'{self.__base_path}/export'


def __remove_dir(path):
    if os.path.exists(path):
        shutil.rmtree(path)
        logger.info(path)


def __create_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)
        logger.info(path)


def run():
    model = Model()
    for path in (model.get_export_path, model.get_result_path):
        __remove_dir(path)
        __create_dir(path)

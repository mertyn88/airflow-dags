from amplitude import properties
from datetime import date, timedelta, datetime
import glob
import logging
import os
import socket

from hdfs import InsecureClient

logger = logging.getLogger()


class Model:
    def __init__(self, dt):
        self.__base_path = f'{properties.base_path}/amplitude'
        if dt is None:
            self.__date = date.today() - timedelta(1)
        else:
            self.__date = datetime.strptime(dt, '%Y%m%d').date()

    @property
    def get_url(self):
        return properties.hdfs_url.replace('{host}', socket.gethostbyname(socket.gethostname()))

    @property
    def get_user(self):
        return properties.hdfs_user

    @property
    def get_files(self):
        return glob.glob(f'{self.__base_path}/result/**', recursive=False)

    @property
    def get_hdfs_path(self):
        return properties.hdfs_copy_path.replace('{year}', f'{self.__date:%Y}').replace('{month}', f'{self.__date:%m}')

    @property
    def get_file_name(self):
        return f'{self.__date:%d}.ndjson'


def run(dt=None):
    model = Model(dt)
    hdfs_client = InsecureClient(model.get_url, user=model.get_user)
    for file in model.get_files:
        path = model.get_hdfs_path.replace('{file}', os.path.basename(file))
        hdfs_client.makedirs(path, permission=None)
        hdfs_client.upload(f'{path}/{model.get_file_name}', file, overwrite=True)

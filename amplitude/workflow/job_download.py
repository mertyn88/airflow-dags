from amplitude import properties
from datetime import date, timedelta, datetime
import io

import requests
import zipfile
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
    def get_url(self):
        return properties.amplitude_url.replace('{date}', f'{self.__date:%Y}{self.__date:%m}{self.__date:%d}')

    @property
    def get_header(self):
        return {"authorization": f"Basic {properties.amplitude_auth_key}"}

    @property
    def get_export_path(self):
        return f'{self.__base_path}/export'


def run(dt=None):
    model = Model(dt)
    logger.info(model.get_url)
    req = requests.get(model.get_url, headers=model.get_header, stream=True)
    zf = zipfile.ZipFile(io.BytesIO(req.content))
    zf.extractall(model.get_export_path)

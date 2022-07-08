from indicators import properties
from indicators import database
from datetime import date, timedelta
import json
import pandas
import requests
import logging
from util import util

logger = logging.getLogger()


class Model:
    def __init__(self):
        self.__date = date.today() - timedelta(1)
        self.__env = util.get_active_profile()

    @property
    def get_url(self):
        return f'{properties.base_url}{{url}}/airflow'

    @property
    def get_date(self):
        return f'{self.__date:%Y}-{self.__date:%m}-{self.__date:%d}'

    @property
    def get_database(self):
        return {
            'database': properties.insert_database,
            'user': properties.insert_user,
            'password': properties.insert_password[util.get_active_profile()],
            'host': properties.insert_host[util.get_active_profile()],
            'port': properties.insert_port,
            'schema': properties.insert_schema
        }


def run(**kwargs) -> bool:
    model = Model()
    # Request & Response
    request_url = model.get_url.replace('{url}', kwargs['url'])
    if kwargs['is_date']:
        request_url += f'/{model.get_date}/{model.get_date}'
    logger.info(request_url)
    response = requests.get(request_url)

    # Convert Dataframe
    frame = pandas.DataFrame.from_dict(json.loads(response.content))
    frame.insert(len(frame.columns), 'event_code', kwargs['event_code'])
    frame.insert(len(frame.columns), 'range_at', model.get_date)

    # Insert Data
    return database.insert_data(
        connect=model.get_database,
        table=kwargs['table'],
        values=frame.to_numpy(),
        columns=frame.columns.values.tolist()
    )

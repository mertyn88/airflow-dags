from amplitude.workflow import job_download
from amplitude.workflow import job_classification
from amplitude.workflow import job_upload
from amplitude.workflow import job_hdfs
from amplitude.workflow import job_remove
import pandas
import logging

# Init logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Format logger
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(
    logging.Formatter('[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d] [%(funcName)s] [%(message)s]'))
logger.addHandler(stream_handler)


def __run(dt):
    # Work 0. Init directory
    job_remove.run()
    # Work 1. Download zip
    job_download.run(dt)
    # Work 2. Classification
    job_classification.run()
    # Work 3. Upload S3
    job_upload.run(dt)
    # Work 4. Copy hdfs
    job_hdfs.run(dt)
    # Work 5. Init directory
    job_remove.run()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    dt_index = pandas.date_range(start='20220516', end='20220614')
    # dt_index = pandas.date_range(start='20220516', end='20220516')
    dt_list = dt_index.strftime('%Y%m%d')
    for dt in dt_list:
        __run(dt)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/

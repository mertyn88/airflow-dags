from indicators.workflow import job_partner_branch
import logging

# Init logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Format logger
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(
    logging.Formatter('[%(asctime)s] [%(levelname)s] [%(filename)s:%(lineno)d] [%(funcName)s] [%(message)s]'))
logger.addHandler(stream_handler)


def __run():
    job_partner_branch.run()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    __run()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/

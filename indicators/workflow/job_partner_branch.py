from indicators import properties
from indicators import database
from datetime import date, timedelta
import json
import pandas
import requests
import logging
from util import util
from indicators.workflow import job_event

logger = logging.getLogger()


def run():
    if not job_event.run(
            url='/partner/branch/import_approve_category',
            table='event_category_daily',
            event_code='BRANCH_IMPORT_APPROVE',
            is_date=True
    ):
        raise Exception('Import approve branch category error!')

    if not job_event.run(
            url='/partner/branch/import_approve_location',
            table='event_location_daily',
            event_code='BRANCH_IMPORT_APPROVE',
            is_date=True
    ):
        raise Exception('Import approve branch location error!')

    if not job_event.run(
            url='/partner/branch/advertise_on_category',
            table='event_category_daily',
            event_code='BRANCH_ADVERTISE_ON',
            is_date=False
    ):
        raise Exception('Advertise on branch category error!')

    if not job_event.run(
            url='/partner/branch/advertise_on_location',
            table='event_location_daily',
            event_code='BRANCH_ADVERTISE_ON',
            is_date=False
    ):
        raise Exception('Advertise on branch location error!')

    if not job_event.run(
            url='/partner/branch/advertise_off_category',
            table='event_category_daily',
            event_code='BRANCH_ADVERTISE_OFF',
            is_date=False
    ):
        raise Exception('Advertise off branch category error!')

    if not job_event.run(
            url='/partner/branch/advertise_off_location',
            table='event_location_daily',
            event_code='BRANCH_ADVERTISE_OFF',
            is_date=False
    ):
        raise Exception('Advertise off branch location error!')

    if not job_event.run(
            url='/partner/branch/advertise_request_category',
            table='event_category_daily',
            event_code='BRANCH_ADVERTISE_REQUEST',
            is_date=True
    ):
        raise Exception('Advertise request branch category error!')

    if not job_event.run(
            url='/partner/branch/advertise_request_location',
            table='event_location_daily',
            event_code='BRANCH_ADVERTISE_REQUEST',
            is_date=True
    ):
        raise Exception('Advertise request branch location error!')

    if not job_event.run(
            url='/partner/branch/advertise_hold_category',
            table='event_category_daily',
            event_code='BRANCH_ADVERTISE_HOLD',
            is_date=True
    ):
        raise Exception('Advertise hold branch category error!')

    if not job_event.run(
            url='/partner/branch/advertise_hold_location',
            table='event_location_daily',
            event_code='BRANCH_ADVERTISE_HOLD',
            is_date=True
    ):
        raise Exception('Advertise hold branch location error!')

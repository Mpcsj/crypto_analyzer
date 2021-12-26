import datetime

DATE_AS_FILENAME_FORMAT = '%Y_%m_%dT%H_%M_%S'


def get_curr_utc_date():
    return datetime.datetime.now(datetime.timezone.utc)


def get_curr_utc_time_to_filename():
    return get_curr_utc_date().strftime(DATE_AS_FILENAME_FORMAT)


def get_datetime_from_filename(filename: str):
    idx_extension = filename.find('.')
    if idx_extension > 0:
        filename = filename[0:idx_extension]
    return datetime.datetime.strptime(filename, DATE_AS_FILENAME_FORMAT)

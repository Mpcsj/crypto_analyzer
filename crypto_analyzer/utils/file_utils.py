import os


def return_n_directories(path: str, n: int) -> str:
    for _ in range(n):
        path = str(os.path.abspath(os.path.join(path, os.pardir)))
    return path


def append_url(base_folder_url: str, *to_append):
    res = base_folder_url
    for partial_path in to_append:
        res = os.path.join(res, partial_path)
    return res


def create_folder_if_not_exists(folder_url):
    if folder_url[-1] == os.sep:
        folder_url = folder_url[:-1]
    if not os.path.exists(folder_url):
        os.makedirs(folder_url)


def get_data_folder_url():
    return os.environ.get('DATA_FOLDER_URL')


def get_extracted_html_folder_url():
    res = append_url(get_data_folder_url(), 'extracted_html')
    create_folder_if_not_exists(res)
    return res


def get_parsed_csv_folder_url():
    res = append_url(get_data_folder_url(), 'parsed_csv')
    create_folder_if_not_exists(res)
    return res


def write_str_file(output_file_url: str, content: str):
    with open(output_file_url, 'w') as writer:
        writer.write(content)


def read_str_file(input_file_url: str) -> str:
    # no streaming for now, just the whole content
    with open(input_file_url, 'r') as reader:
        res = reader.read()
    return res


def get_latest_file_url(folder_url: str):
    from crypto_analyzer.utils import date_utils
    latest_file_url = None
    latest_filename_date = None
    for filename in os.listdir(folder_url):
        curr_filename_date = date_utils.get_datetime_from_filename(filename)
        curr_file_url = append_url(folder_url, filename)
        if latest_file_url is None:
            latest_file_url = curr_file_url
            latest_filename_date = curr_filename_date
        else:
            if curr_filename_date > latest_filename_date:
                latest_file_url = curr_file_url
                latest_filename_date = curr_filename_date
    return latest_file_url

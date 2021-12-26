import pathlib


def get_current_folder_url():
    path = str(pathlib.Path(
        __file__).parent.absolute())  # current path of this file
    return path

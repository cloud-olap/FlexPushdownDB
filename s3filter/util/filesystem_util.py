import os

__author__ = "Abdurrahman Ghanem <abghanem@qf.org.qa>"


def create_dirs(path):
    if not os.path.exists(path):
        try:
            os.makedirs(path)
        except OSError as e:
            print('Directory exists. Error: {}'.format(e.message))


def create_file_dirs(path):
    dir_path = os.path.dirname(path)
    if not os.path.exists(dir_path):
        try:
            os.makedirs(dir_path)
        except OSError as e:
            print('File already exists. Error: {}'.format(e.message))

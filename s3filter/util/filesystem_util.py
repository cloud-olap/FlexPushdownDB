import os

__author__ = "Abdurrahman Ghanem <abghanem@qf.org.qa>"


def create_dirs(path):
    if not os.path.exists(path):
        os.makedirs(path)


def create_file_dirs(path):
    dir_path = os.path.dirname(path)
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

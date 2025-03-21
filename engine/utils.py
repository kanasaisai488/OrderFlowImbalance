# engine/utils.py
import os

def get_project_root():
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

def get_data_path(*subdirs):
    return os.path.join(get_project_root(), "data", *subdirs)
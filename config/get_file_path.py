import os


def get_file_path(file):
    script_dir = os.path.dirname(__file__)
    return os.path.join(script_dir, file)

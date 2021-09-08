import configparser
import os

script_dir = os.path.dirname(__file__)
rel_path = "kafka_config.ini"
abs_file_path = os.path.join(script_dir, rel_path)
conf = configparser.ConfigParser()
conf.read(abs_file_path)
config = {s: dict(conf.items(s)) for s in conf.sections()}


def get_app_config():
    return config.get('APP')

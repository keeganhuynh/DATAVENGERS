import os
import yaml


class ConfigLoader:
    def __init__(self):
        config_file_path = os.path.join(os.getcwd(), 'src/config.yaml')
        try:
            config_data = None
            file = open(config_file_path, 'r')
            config_data = yaml.safe_load(file)
        except Exception as ex:
            print(f'Can not load config file, error: {ex}')
        self.config_data = config_data
        
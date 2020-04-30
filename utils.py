import pathlib
import yaml


def parse_config(config_path):
    path = pathlib.Path(config_path)
    if path.suffix == '.yaml' or path.suffix == '.yml':
        try:
            with open(config_path) as f:
                config = yaml.load(f, Loader=yaml.FullLoader)
        except:
            raise ValueError('Failed to read YAML config')
    # TODO: add support for json and/or ini config format
    return config

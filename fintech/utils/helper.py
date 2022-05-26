import pkgutil
import yaml


def read_meta(package, file_name, prefix='config/'):
    return yaml.load(pkgutil.get_data(package, f"{prefix}{file_name}.yml"))

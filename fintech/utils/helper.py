import os
import yaml
import time
import pkgutil
import shutil

import pandas as pd


def read_meta(package, file_name, prefix='config/'):
    return yaml.load(pkgutil.get_data(package, f"{prefix}{file_name}.yml"))


def move_processed_file(src_path, dest_path, file):
    dest_file_path = f'{dest_path}/{file}'
    src_file_path = f'{src_path}/{file}'
    if not os.path.exists(dest_file_path):
        shutil.copyfile(src_file_path, dest_file_path)
    else:
        base, extension = os.path.splitext(file)
        i = 1
        while os.path.exists(os.path.join(dest_path, '{}_{}{}'.format(base, i, extension))):
            i += 1
        shutil.copy(src_file_path, os.path.join(dest_path, '{}_{}{}'.format(base, i, extension)))
    os.remove(f'{src_path}/{file}')


def sorting_files_on_modification_dt(src_path, file_list):

    sorted_files = pd.DataFrame({'files': file_list})
    modified_date = []
    for f in file_list:
        modified_epoch = os.path.getmtime(f'{src_path}/{f}')
        modified_date.append(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(modified_epoch)))
    sorted_files['modified_date'] = modified_date
    return sorted_files.sort_values(by='modified_date', ascending=True)

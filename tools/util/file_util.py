import csv
import os

import pandas as pd

_cur_dir = os.path.dirname(__file__)
_root_dir = os.path.dirname(_cur_dir)


def empty_csv(file):
    with open(file, "w", newline="") as csvfile:
        # writer = csv.writer(csvfile)
        # writer.writerow("")
        pass

def write_row_csv(row, file):
    with open(file, 'a', newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([row])


def write_lst_csv(lstData, file, isOverride):
    way = "a"
    if (isOverride == 1):
        way = "w"

    arr = []
    for data in lstData:
        arr.append([data])
    with open(file, way, newline="") as csvfile:
        writer = csv.writer(csvfile, delimiter='\t', quoting=csv.QUOTE_NONE)
        writer.writerows(arr)


def read_first_csv(file_path):
    try:
        df = pd.read_csv(file_path, header=None)
        return df.iloc[:, 0].tolist()  # 读取第一列的数据
    except Exception as e:
        print(f"Error reading from csv: {e}")


def get_full_path(relative_path):
    return _root_dir + relative_path


def get_root_path():
    return _root_dir


def get_run_path():
    return os.getcwd()


def get_run_path_file(file):
    return os.path.join(get_run_path(), file)


def join(path, file):
    os.path.join(path, file)

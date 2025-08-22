import csv
import os

import pandas as pd

# _cur_dir = os.path.dirname(__file__)
_root_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))


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
        writer = csv.writer(csvfile,escapechar='\"', quoting=csv.QUOTE_NONE)
        writer.writerows(arr)

def write_lst_txt(lstData, file, isOverride=0):
    way = "a"
    if (isOverride == 1):
        way = "w"

    arr_str = [item if isinstance(item, str) else str(item) for item in lstData]
    with open(file, way, encoding='utf-8') as f:
        f.write('\n'.join(arr_str))

def write_txt(text, file, isOverride=0):
    way = "a"
    if (isOverride == 1):
        way = "w"
    with open(file, way, encoding='utf-8') as f:
        f.write(text)

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

def get_files_from_dir(dir_path):
    files= []
    for filename in os.listdir(dir_path):
        filepath = os.path.join(dir_path, filename)
        if os.path.isfile(filepath):
            files.append(filepath)
    return files

def read_file_to_string(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            content = file.read()
        return content
    except FileNotFoundError:
        print(f"错误：文件 '{file_path}' 不存在。")
    except IOError:
        print(f"错误：无法读取文件 '{file_path}'。")
    except Exception as e:
        print(f"发生未知错误：{str(e)}")
    return None

def read_txt_to_list(file_path):
    """
    读取文本文件，按行存入列表并返回
    :param file_path: 文件路径
    :return: 行列表
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    # 去除每行末尾的换行符
    return [line.rstrip('\n') for line in lines]

import json
import os
import re

import pandas as pd
from tools.util import file_util as fileUtil

_cur_dir = os.path.dirname(__file__)
_root_dir = os.path.dirname(_cur_dir)


def toIn():
    file = os.path.join(_cur_dir, 'f0_result.csv')
    df = pd.read_csv(file, header=None)
    lstData = df.iloc[:, 0].tolist()
    lstUrl = ["in("]
    for i in range(len(lstData)):
        if i == len(lstData) - 1:
            url = "'" + lstData[i] + "'"
        else:
            url = "'" + lstData[i] + "',"
        lstUrl.append(url)
    lstUrl.append(')')
    fileUtil.write_lst_csv(lstUrl, file, 1)


def toInInt():
    file = os.path.join(_cur_dir, 'f0_result.csv')
    df = pd.read_csv(file, header=None)
    lstData = df.iloc[:, 0].tolist()
    lstUrl = ["in("]
    for i in range(len(lstData)):
        if i == len(lstData) - 1:
            value = lstData[i]
        else:
            value = str(lstData[i]) + ","
        lstUrl.append(value)
    lstUrl.append(')')
    fileUtil.write_lst_csv(lstUrl, file, 1)


def toUnionSelct(fieldName):
    file = os.path.join(_cur_dir, 'f0_result.csv')
    df = pd.read_csv(file, header=None)
    lstData = df.iloc[:, 0].tolist()
    lstUrl = []
    for i in range(len(lstData)):
        if i == 0:
            url = f"select '{lstData[i]}' as {fieldName}"
        else:
            url = f"union all select '{lstData[i]}'"
        lstUrl.append(url)
    fileUtil.write_lst_csv(lstUrl, file, 1)


def snake_to_camel(snake_str):
    return re.sub(r'_([a-z])', lambda m: m.group(1).upper(), snake_str)


def camel_to_snake(camel_str):
    pattern = re.compile(r'(?<!^)(?=[A-Z])')
    return pattern.sub('_', camel_str).lower()


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


def toLine():
    # file = os.path.join(_cur_dir, 'f0_result.csv')
    # df = pd.read_csv(file, header=None)
    # lstData = df.iloc[:, 0].tolist()
    # lstData=read_file_to_string(file)
    all_items = os.listdir(_cur_dir + '/msg')
    # 过滤出文件（排除文件夹）
    files = [item for item in all_items if os.path.isfile(os.path.join(_cur_dir + '/msg', item))]
    fileUtil.empty_csv('f0_result.csv')
    lstData = []
    for file in files:
        if '.txt' not in file:
            continue
        try:
            jsonData = []
            data = read_file_to_string(_cur_dir + '/msg/' + file)
            data = data.replace("'", "\"")
            jsonData = json.loads(data)
            # with open(os.path.join(_cur_dir + '/msg', file), 'r') as data:
            #     jsonData = json.load(data)
            # lstUrl = []
            # for url in jsonData:
            #     lstUrl.append(url)
            # fileUtil.write_lst_csv(jsonData, 'f0_result.csv', 0)
            print(f'{file}:{len(jsonData)}')
            for item in jsonData:
                if item not in lstData:
                    lstData.append(item)
            # lstData = list(set(lstData))
        except Exception as ex:
            print(file)
    fileUtil.write_lst_csv(lstData, 'f0_result.csv', 0)
    print(f'{len(lstData)}')

# 生成 SQL
def json_to_sql(table_name):
    with open('f0_result.csv', 'r', encoding='utf-8') as file:
        json_data = file.read()
        # 解析 JSON 数据
        data = json.loads(json_data)

        # 开始构建 SQL 语句
        sql = f"CREATE TABLE {table_name} (\n"

        # 遍历 JSON 对象的键值对
        columns = []
        for key, value in data.items():
            # 将 JSON 键转换为有效的 SQL 列名
            column_name = re.sub(r'\W+', '_', key).lower()

            # 根据值类型确定 SQL 数据类型
            if isinstance(value, int):
                data_type = "INT"
            elif isinstance(value, float):
                data_type = "FLOAT"
            elif isinstance(value, bool):
                data_type = "BOOLEAN"
            elif isinstance(value, str):
                # 对于字符串，我们假设最大长度为 255
                data_type = "VARCHAR(255)"
            elif value is None:
                data_type = "VARCHAR(255)"
            else:
                # 对于复杂类型（如嵌套对象或数组），我们使用 TEXT
                data_type = "TEXT"

            columns.append(f"    {column_name} {data_type}")

        # 将所有列定义连接起来
        sql += ",\n".join(columns)

        # 添加主键（这里我们假设第一个字段为主键）
        first_column = re.sub(r'\W+', '_', list(data.keys())[0]).lower()
        sql += f",\n    PRIMARY KEY ({first_column})"

        # 结束 SQL 语句
        sql += "\n);"

        print(sql)

if __name__ == "__main__":
    # file = os.path.join(_cur_dir, 'f0_result.csv')
    # df = pd.read_csv(file, header=None)
    # lstData = df.iloc[:, 0].tolist()
    # urls = list(set(lstData))
    # print(len(urls))
    # toIn()
    # toInInt()
    # toUnionSelct('user_id')
    # print(snake_to_camel("h_es_link_index_lang"))
    json_to_sql('user')
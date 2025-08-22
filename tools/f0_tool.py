import asyncio
import json
import os
import re
from datetime import datetime, timedelta

import pandas as pd
import requests
from pyrogram import Client

from tools.model.entities import AppGroup
from tools.util import file_util as fileUtil, db_helper
from tools.util.file_util import get_files_from_dir

_cur_dir = os.path.dirname(__file__)
_root_dir = os.path.dirname(_cur_dir)

def toIn():
    file = os.path.join(_cur_dir, 'f0_source.csv')
    df = pd.read_csv(file, header=None)
    lstData = df.iloc[:, 0].tolist()
    lstUrl = ["in("]
    for i in range(len(lstData)):
        if i == len(lstData) - 1:
            url = f"'" + str(lstData[i]) + "'"
        else:
            url = "'" + str(lstData[i]) + "',"
        lstUrl.append(url)
    lstUrl.append(')')
    fileUtil.write_lst_txt(lstUrl, os.path.join(_cur_dir, 'f0_result.txt'), 1)

def toInInt():
    file = os.path.join(_cur_dir, 'f0_result.txt')
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
    file = os.path.join(_cur_dir, 'f0_source.csv')
    df = pd.read_csv(file, header=None)
    lstData = df.iloc[:, 0].tolist()
    lstUrl = []
    for i in range(len(lstData)):
        if i == 0:
            url = f"select '{lstData[i]}' as {fieldName}"
        else:
            url = f"union all select '{lstData[i]}'"
        lstUrl.append(url)
    fileUtil.write_lst_csv(lstUrl, os.path.join(_cur_dir, 'f0_result.txt'), 1)

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
    # file = os.path.join(_cur_dir, 'f0_result.txt')
    # df = pd.read_csv(file, header=None)
    # lstData = df.iloc[:, 0].tolist()
    # lstData=read_file_to_string(file)
    all_items = os.listdir(_cur_dir + '/msg')
    # 过滤出文件（排除文件夹）
    files = [item for item in all_items if os.path.isfile(os.path.join(_cur_dir + '/msg', item))]
    fileUtil.empty_csv('f0_result.txt')
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
            # fileUtil.write_lst_csv(jsonData, 'f0_result.txt', 0)
            print(f'{file}:{len(jsonData)}')
            for item in jsonData:
                if item not in lstData:
                    lstData.append(item)
            # lstData = list(set(lstData))
        except Exception as ex:
            print(file)
    fileUtil.write_lst_csv(lstData, 'f0_result.txt', 0)
    print(f'{len(lstData)}')

# 生成 SQL
def json_to_sql(table_name):
    with open('f0_result.txt', 'r', encoding='utf-8') as file:
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
                data_type = "VARCHAR(100)"
            elif value is None:
                data_type = "VARCHAR(100)"
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

# def snake_to_camel(snake_str: str) -> str:
#     parts = snake_str.split('_')
#     return parts[0] + ''.join(word.capitalize() for word in parts[1:])

def to_resultmap():
    lstSource = fileUtil.read_first_csv(fileUtil.get_run_path_file('f0_source.csv'))

    lstResult = ['<resultMap type="CmsContentListen" id="CmsContentListenResult">']
    for i in range(len(lstSource)):
        value = f'<result property="{snake_to_camel(lstSource[i])}" column="{lstSource[i]}"/>'
        lstResult.append(value)
    lstResult.append('</resultMap>')
    fileUtil.write_lst_txt(lstResult, fileUtil.get_run_path_file('f0_result.txt'), 1)


async def import_listen():
    try:
        await db_helper.init_db()

        files = fileUtil.get_files_from_dir(fileUtil.get_full_path("/tools/appid"))
        for file in files:
            content = fileUtil.read_file_to_string(file)
            arr = json.loads(content)
            appGroups = []
            app_id = file.split('/')[-1].split('-')[0]
            for item in arr:
                appGroup = AppGroup()
                appGroup.url = f'https://t.me/{item}'
                appGroup.app_id = app_id
                appGroups.append(appGroup)
            # 批量插入
            await AppGroup.bulk_create(appGroups)
    finally:
        await db_helper.close_db()

async def get_chat_history():
    async with Client(name="bot1", api_id=29396219,
                      api_hash="da88a5e5f0bd0a69f25d94516d3c87e6",
                      session_string="BAHAjPsAnVM4a5VeNlF3yNbzxL3Bb2Zyqpb_s_rS_akXBFUBSwt2rQIEuI3swMRIdLTlUMAiH80bEqQx08B_-HGyAch5vQGGZwLZwSW9wYyB5_e8y1lqC30WUygFEU_lSBlAPofoFjCQAgRjjFkp9X0u4Zlqs8NzqsuMpUEdaGoVvX0SL6w0cpi2JCoA6GtImPaWyzag46Osh_lM7nSVmoPf5Db1S9Zo7HrkmoEcRUBp9RFkwwgKXmTnzMQi57RjdYJ3ojF8RBNCOENhDT4oHlHwS5VTQTQxvRe5cY9zh8pk1qwL4wfpE2o1XpPT710Os9ISHH5A6OIJhpfWvX3lzbNLN7gmNQAAAAHov2NQAA"
                      ) as bot:
        targets = [
            "@tgcnlib"
        ]
        cmsApi = "http://a5b684db7f9d746b18f60bbe60781a74-8b898802e7cba134.elb.ap-southeast-1.amazonaws.com:8800/cms/content/collectMsgInfo"
        headers = {
            'Content-Type': 'application/json'
        }
        one_month = datetime.now() - timedelta(days=7)
        for target in targets:
            arr = []
            url_prefix = f"https://t.me/{target.replace('@', '')}"
            async for message in bot.get_chat_history(chat_id=target, offset_date=one_month):
                url = f"{url_prefix}/{message.id}"
                msg_txt = ""
                if hasattr(message, 'text') and message.text:
                    msg_txt = message.text
                if hasattr(message, 'caption') and message.caption:
                    msg_txt = message.caption
                if msg_txt == "":
                    continue
                date_last = message.date
                formatted_str = date_last.strftime('%Y-%m-%d %H:%M:%S')
                link_type = 4
                read_num = None
                file_duration = None
                file_size = None
                file_name = None
                if hasattr(message, 'service'):
                    if message.service:
                        link_type = None
                if hasattr(message, 'views') and message.views:
                    read_num = message.views
                # 2- Vedio视频消息 3- Photo图片消息 4- Text文本消息 5- Audio音乐消息 6- File文件消息
                if hasattr(message, 'media') and message.media:
                    message_type = str(message.media)
                    if message_type == "MessageMediaType.PHOTO":
                        link_type = 3
                        if hasattr(message, 'photo') and message.photo:
                            photo_obj = message.photo
                            file_size = photo_obj.file_size
                    if message_type == "MessageMediaType.VIDEO":
                        link_type = 2
                        if hasattr(message, 'video') and message.video:
                            video_obj = message.video
                            file_duration = video_obj.duration
                            file_size = video_obj.file_size
                            file_name = video_obj.file_name
                    if message_type == "MessageMediaType.AUDIO":
                        link_type = 5
                        if hasattr(message, 'audio') and message.audio:
                            audio_obj = message.audio
                            file_duration = audio_obj.duration
                            file_size = audio_obj.file_size
                            file_name = audio_obj.file_name
                    if message_type == "MessageMediaType.DOCUMENT":
                        link_type = 6
                        if hasattr(message, 'document') and message.document:
                            document_obj = message.document
                            file_size = document_obj.file_size
                            file_name = document_obj.file_name
                json_data = {
                    "url": url,
                    "status": 1,
                    "linkType": link_type,
                    "publishTime": formatted_str,
                    "readNum": read_num,
                    "duration": file_duration,
                    "fileSize": file_size,
                    "fileName": file_name,
                    "text": msg_txt
                }
                arr.append(json_data)
                if len(arr) == 100:
                    response = requests.post(cmsApi, headers=headers, json=arr, timeout=60)
                    result = json.loads(response.content.decode('utf-8'))
                    print(f'{datetime.now()},{result}')
                    arr = []
            if len(arr) > 0:
                response = requests.post(cmsApi, headers=headers, json=arr, timeout=60)
                result = json.loads(response.content)
                print(f'{datetime.now()},{result}')

def remove_url(str):
    if not str:
        return str

    # 正则表达式匹配超链接
    pattern = r'(https?://[0-9a-zA-Z_+-.]+)|(@[0-9a-zA-Z_+-]+)'
    str = re.sub(pattern, "", str)
    print(str)
    return str

def clean_text(text: str) -> str:
    """
    Remove all links and @usernames from the given string.
    """
    # Remove URLs (http, https, www, etc.)
    text = re.sub(r'https?://\S+|www\.\S+|t\.me/\S+', '', text)
    # Remove @usernames (e.g., @someone)
    text = re.sub(r'@\w+', '', text)
    # Remove extra spaces/newlines
    # text = re.sub(r'\s+', ' ', text).strip()
    return text

if __name__ == "__main__":
    # file = os.path.join(_cur_dir, 'f0_result.txt')
    # df = pd.read_csv(file, header=None)
    # lstData = df.iloc[:, 0].tolist()
    # urls = list(set(lstData))
    # print(len(urls))
    # toIn()
    # toInInt()
    # toUnionSelct('user_name')
    # print(snake_to_camel("h_es_link_index_lang"))
    # json_to_sql('user')
    # print(dir(Tortoise))
    # to_resultmap()
    # asyncio.run(import_listen())
    # asyncio.run(get_chat_history())
    remove_url("疼@abc发的https://t.me dabe")

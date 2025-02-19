import os
import re
import sqlite3
import threading
import time
import uuid as uu
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from datetime import timedelta, timezone

import langid
import pandas as pd
import pytz
import requests
from bs4 import BeautifulSoup
from langdetect import LangDetectException
from pyrogram import Client
from pyrogram.enums import ParseMode
from tools.util import file_util as fileUtil


def excel_import():
    _dir = os.path.dirname(__file__)
    file_path = os.path.join(_dir, 'geturl.xlsx')
    excel_file = pd.ExcelFile(file_path)
    # 获取所有工作表的名称
    sheet_names = excel_file.sheet_names
    # 遍历每个工作表并读取数据

    # 读取工作表数据
    df = excel_file.parse("Sheet1", header=None)
    num_columns = len(df.columns)
    file = os.path.join(_dir, 'f0_result.csv')
    fileUtil.empty_csv(file)
    header = []
    for rowIndex, row in df.iterrows():
        result = []
        for colIndex in range(num_columns):
            if rowIndex == 0:
                header.append(row[colIndex])
            else:
                col = str(row[colIndex])
                if col == "nan" or col == None or ("'" in col) or (" " in col) or ("\n" in col):
                    continue
                obj = f"insert into tmp_kw(name,category) values('{row[colIndex]}','{header[colIndex]}');"
                result.append(obj)
        try:
            fileUtil.write_lst_csv(result, file, 0)
        except Exception as ex:
            print(str(rowIndex) + str(result))
            break


if __name__ == "__main__":
    excel_import()

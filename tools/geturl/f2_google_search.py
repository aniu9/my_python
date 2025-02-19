import csv
import datetime
import os
import random
import time

import pandas as pd
import requests
from googlesearch import search

_cur_dir = os.path.dirname(__file__)

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

def get_url(query):
    try:
        # 抓取谷歌搜索结果中的URL
        urlAll = []
        urls = []
        # random.uniform(2, 5)
        results = search(query, num_results=30)
        for url in results:  # num_results参数指定要抓取的结果数量
            urlAll.append(url)
            if "+" not in url:
                urls.append(url)
        if not urls:
            print(f'{datetime.datetime.now()},未找到数据:{query}')
            return
        # 输出抓取到的URL
        result_file = _cur_dir + '/f2_result.csv'
        write_lst_csv(urls, result_file, 0)
        time.sleep(random.randrange(30, 60))
    except requests.RequestException as e:
        print(f'{datetime.datetime.now()},休眠30分钟')
        time.sleep(60 * 30)


if __name__ == "__main__":
    kw_file = _cur_dir + '/f2_keyword.csv'
    lst_kw = read_first_csv(kw_file)
    count = 0
    if lst_kw is not None:
        for kw in lst_kw:
            count = count + 1
            if count <= 100:
                continue
            if count > 100:
                break

            query = "site:t.me " + kw + " https://t.me"  # 搜索查询词
            get_url(query)
            query = 'inurl:"https://t.me" intext:"' + kw + '"'  # 搜索查询词
            get_url(query)
            print(f'{datetime.datetime.now()},count:{count},{kw}')
            if count % 5 == 0:
                time.sleep(random.randrange(60, 90))

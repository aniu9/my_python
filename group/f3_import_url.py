"""
查询telegram_data.db 数据库
"""

import os
from datetime import datetime

import pandas as pd
import requests


def query_telegram_data():
    cur_dir = os.path.dirname(__file__)
    # conn = sqlite3.connect(root_dir + '/telegram_data.db')
    # c = conn.cursor()
    # c.execute('''
    #     SELECT url, lang_type FROM url_data
    # ''')
    # data_link = c.fetchall()
    # conn.close()
    file_path = cur_dir + '/f3_import_url.csv'
    lstData = []
    # with open(file_path, 'r') as file:
    #     csv_data = csv.DictReader(file)
    #     data = list(csv_data)
    #     json_data = json.dumps(data)
    #     lstData = json.loads(json_data)

    df = pd.read_csv(file_path, header=None)
    lstData = df.iloc[:, 0].tolist()

    return lstData


def main(langType):
    # 查询数据
    data_link = query_telegram_data()
    row_count = len(data_link)
    # print(f"Found {row_count} rows in the database.")
    if row_count > 0:
        # url = "http://localhost:8080/cms/content/sync"
        url = "http://a5b684db7f9d746b18f60bbe60781a74-8b898802e7cba134.elb.ap-southeast-1.amazonaws.com:8800/cms/content/sync"
        headers = {
            "appKey": "90b0e5c2eb0d11eeba8ee49d6628936c",
            "Content-Type": "application/json"
        }
        size = 0
        count=0
        data_list = []
        for item in data_link:
            size = size + 1
            count=count+1
            data = {
                "url": item,
                "langType": langType
            }
            data_list.append(data)
            if size >= 200:
                response = requests.post(url, headers=headers, json=data_list)
                print(f"{datetime.now()},count:{count}")
                print(response.text)
                data_list = []
                size = 0
        if size > 0:
            requests.post(url, headers=headers, json=data_list)
        print(f"{datetime.now()},Data has been saved:{len(data_link)}")
    else:
        print("No data found in the database.")


if __name__ == '__main__':
    main(1)

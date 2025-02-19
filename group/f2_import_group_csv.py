"""
查询telegram_data.db 数据库
"""
import csv
import json
import os
import time
from datetime import datetime

import requests


def query_telegram_data():
    cur_dir = os.path.dirname(__file__)
    file_path = cur_dir + '/f2_import_group.csv'
    lstData = []
    with open(file_path, 'r') as file:
        csv_data = csv.DictReader(file)
        data = list(csv_data)
        json_data = json.dumps(data)
        lstData = json.loads(json_data)
    return lstData


def main():
    data_link = query_telegram_data()
    row_count = len(data_link)
    print(f"Found {row_count} rows in the database.")
    if row_count > 0:
        # url = "http://localhost:8080/cms/content/sync"
        url = "http://a5b684db7f9d746b18f60bbe60781a74-8b898802e7cba134.elb.ap-southeast-1.amazonaws.com:8800/cms/content/sync"
        headers = {
            "appKey": "90b0e5c2eb0d11eeba8ee49d6628936c",
            "Content-Type": "application/json"
        }
        size = 0
        count = 0
        data_list = []
        for item in data_link:
            size = size + 1
            count = count + 1
            data = {
                "url": item["url"],
                "linkType": item["url_type"],
                "langType": item["language"]
            }
            json = {
                "name": item["title"],
                "description": item["description"],
                "userNum": item["members"]
            }
            if json["userNum"] is None:
                json["userNum"] = 0
            if json["name"] is None or json["name"] == "" or int(json["userNum"]) < 100:
                continue
            if data["linkType"] == 'Group':
                data["linkType"] = 0
            elif data["linkType"] == 'Channel':
                data["linkType"] = 1
            else:
                continue

            if data["langType"] == 'zh-cn':
                data["langType"] = 1
            elif data["langType"] == 'en':
                data["langType"] = 2
            else:
                continue

            data["extendJson"] = json
            data_list.append(data)
            if size >= 200:
                response = requests.post(url, headers=headers, json=data_list)
                print(f"{datetime.now()},count:{count}")
                print(response.text)
                data_list = []
                size = 0
        if size > 0:
            requests.post(url, headers=headers, json=data_list)
        print(f"{datetime.now()},Data has been saved")

        time.sleep(2)
        url = "http://a7a4a32172a574102a42076ffca608eb-7ff1940d50a42959.elb.ap-southeast-1.amazonaws.com:8800/rule/test/batchPullData"
        json_param = {
            "functionNames": "cms_group_rule,cms_group_rule_en",
            "isRun": 1
        }
        requests.post(url, headers=headers, json=json_param)
        print(f"{datetime.now()},Data has been run")
    else:
        print("No data found in the database.")


if __name__ == '__main__':
    main()

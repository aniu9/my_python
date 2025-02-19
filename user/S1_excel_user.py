import os
from datetime import datetime

import pandas as pd
import requests


def read_urls_from_excel(file_path):
    try:
        # 读取 Excel 文件
        excel_file = pd.ExcelFile(file_path)

        # 获取所有工作表的名称
        sheet_names = excel_file.sheet_names

        # 循环读取每个工作表的每一行每一列数据
        data_lst = []
        for sheet_name in sheet_names:
            # 读取当前工作表的数据
            df = excel_file.parse(sheet_name)

            # 循环读取每一行数据
            for index, row in df.iterrows():
                # 循环读取每一列数据
                json_data = {}
                for column in df.columns:
                    # 获取当前单元格的数据
                    json_data[column] = str(row[column])
                data_lst.append(json_data)
        return data_lst
    except Exception as e:
        print(f"Error reading URLs from Excel: {e}")
        return []


def main():
    root_dir = os.path.dirname(os.path.dirname(__file__))
    data_lst = read_urls_from_excel(root_dir + "/user/tele_user.xls")
    data_push = []
    cnt = 0
    headers = {
        "appKey": "90b0e5c2eb0d11eeba8ee49d6628936c",
        "Content-Type": "application/json"
    }
    url = "http://localhost:8082/umgt/test/handRuleResult"

    for item in data_lst:
        json_data = {
            "actionType": 0,
            "dataObject": "User",
            "objectType": 0,
            "properties": {
                "oid": item["tele_id"],
                "name": item["name"],
                "nickName": item["nick_name"],
                "status": 0
            }
        }
        data_push.append(json_data)
        cnt = cnt + 1
        if cnt % 100 == 0:
            json_param = {
                "ruleResult": {
                    "bsData": data_push
                }
            }
            requests.post(url, headers=headers, json=json_param)
            data_push = []
            print(f"{datetime.now()},已完成：{cnt}")
    if len(data_push) > 0:
        json_param = {
            "ruleResult": {
                "bsData": data_push
            }
        }
        requests.post(url, headers=headers, json=json_param)
    print(f"{datetime.now()},已结束：{cnt}")
    return


if __name__ == "__main__":
    main()

import os

import pandas as pd
import requests


def csv_import():
    _dir = os.path.dirname(__file__)
    file_path = os.path.join(_dir, 'f1_import_es_dict.csv')
    data_list = []
    try:
        df = pd.read_csv(file_path, header=None)
        data_list = df.iloc[:, 0].tolist()  # 读取第一列的URL
    except Exception as e:
        print(f"Error reading URLs from Excel: {e}")

    # url = "http://localhost:8080/cms/keyword/importEsExtKw"
    # url = "http://a5b684db7f9d746b18f60bbe60781a74-8b898802e7cba134.elb.ap-southeast-1.amazonaws.com:8800/cms/keyword/importEsExtKw"
    headers = {
        "appKey": "90b0e5c2eb0d11eeba8ee49d6628936c"
    }
    response = requests.post(url, headers=headers, json=data_list)
    print(response.text)


if __name__ == "__main__":
    csv_import()

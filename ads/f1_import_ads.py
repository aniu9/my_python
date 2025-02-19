"""
查询telegram_data.db 数据库
"""

import os
import sqlite3
import uuid
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup
from src.group.f1_CjGroupInfo import convert_subscribers


def verify_telegram_urls(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    cookies = {
        # 在这里添加需要的Cookies
    }
    responseContent = {}
    success_count = 0
    userNum = None
    linkType = 1
    try:
        response = requests.get(url, headers=headers, cookies=cookies, timeout=10)
        if response.status_code != 200:
            responseContent[url] = {"error": f"Status code: {response.status_code}"}
            return responseContent, success_count

        soup = BeautifulSoup(response.content, 'html.parser')

        def get_content_by_class(soup, class_name):
            element = soup.find('div', class_=class_name)
            return element.text.strip() if element else None

        def get_content_from_alternative(soup, class_name):
            element = soup.find('div', class_=class_name)
            if element:
                return element.get_text(separator=' ').strip()
            return None

        page_extra = get_content_by_class(soup, 'tgme_page_extra') or get_content_from_alternative(soup,
                                                                                                   'tgme_channel_info_counters')
        if page_extra:
            if 'members' in page_extra or 'subscribers' in page_extra:
                # 提取数量并进行转换
                number_str = page_extra.split('members')
                linkType = 0
                if 'subscribers' in page_extra:
                    linkType = 1
                    number_str = page_extra.split('subscribers')
                _number_str = number_str[0]
                userNum = convert_subscribers(_number_str)


    except requests.RequestException as e:
        print(e)

    return userNum, linkType


def query_telegram_data():
    root_dir = os.path.dirname(os.path.dirname(__file__))
    conn = sqlite3.connect(root_dir + '/telegram_data.db')
    c = conn.cursor()
    c.execute('''
        SELECT 广告链接, 广告名称,索引词,生效时间,失效时间
            ,case 广告类型 when '关键词排名广告' then 0 when '群组置顶消息广告' then 1 when '搜索结果顶部广告' then 2
                when '搜索结果底部广告' then 3 when '品牌专页广告' then 4 else null end '广告类型'
        FROM ads where 广告状态='推广中' and 广告类型!='品牌专页广告'
    ''')
    data_link = c.fetchall()

    conn.close()
    return data_link


def query_excel_data():
    _dir = os.path.dirname(__file__)
    file_path = os.path.join(_dir, 'f1_import_ads.xlsx')
    excel_file = pd.ExcelFile(file_path)
    # 获取所有工作表的名称
    sheet_names = excel_file.sheet_names
    # 读取工作表数据
    df = excel_file.parse(sheet_names[0])
    data_link = []
    for index, row in df.iterrows():
        if row["广告状态"] != '推广中' or row["广告类型"] == "品牌专页广告":
            continue

        adsType = None
        if row["广告类型"] == "关键词排名广告":
            adsType = 0
        elif row["广告类型"] == "群组置顶消息广告":
            adsType = 1
        elif row["广告类型"] == "搜索结果顶部广告":
            adsType = 2
        elif row["广告类型"] == "搜索结果底部广告":
            adsType = 3
        elif row["广告类型"] == "品牌专页广告":
            adsType = 4
        data = [row["广告链接"], row["广告名称"], row["索引词"], row["生效时间"], row["失效时间"], adsType]
        data_link.append(data)

    return data_link


def main():
    # 查询数据
    # data_link = query_telegram_data()
    data_link = query_excel_data()
    row_count = len(data_link)
    print(f"Found {row_count} rows in the database.")
    if row_count > 0:
        # url = "http://localhost:8080"
        server = "http://a5b684db7f9d746b18f60bbe60781a74-8b898802e7cba134.elb.ap-southeast-1.amazonaws.com:8800"
        url = server + "/cms/ads/sync"
        headers = {
            "appKey": "90b0e5c2eb0d11eeba8ee49d6628936c",
            "Content-Type": "application/json"
        }
        size = 0
        count = 0
        data_list = []
        for item in data_link:
            userNum, linkType = verify_telegram_urls(item[0])
            size = size + 1
            count = count + 1
            billNo = str(uuid.uuid4())
            searchIndex = str(item[2])
            orderNo = 0
            if item[5] == 0:
                arrStr = searchIndex.split("【")
                searchIndex = arrStr[0]
                orderNo = arrStr[1].replace("】", "")
            if searchIndex == "nan":
                searchIndex = ""
            data = {
                "url": item[0],
                "billNo": billNo,
                "adsChannel": 0,
                "adsCategory": item[5],
                "name": item[1],
                "searchIndex": searchIndex,
                "orderNo": orderNo,
                "linkType": linkType,
                "status": 0,
                "expiredTime": str(item[4]),
                "startTime": str(item[3]),
                "userNum": userNum
            }
            data_list.append(data)
            if size >= 200:
                response = requests.post(url, headers=headers, json=data_list)
                print(f"{datetime.now()},count:{count}")
                print(response.text)
                data_list = []
                size = 0
        if size > 0:
            response = requests.post(url, headers=headers, json=data_list)
        print(f"{datetime.now()},Data has been saved")
        print(response.text)

        url = server + "/cms/geturl/pushAds"
        data_list = {
            "pageNum": 0,
            "pageSize": 1000
        }
        response=requests.post(url, headers=headers, json=data_list)
        print(f"{datetime.now()},Data has been push")
        print(response.text)
    else:
        print("No data found in the database.")


if __name__ == '__main__':
    main()

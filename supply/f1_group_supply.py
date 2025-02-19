import json
import os
import sqlite3
import uuid as uu
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
from bs4 import BeautifulSoup

import f1_group_rabbit as groupRabbit

_cur_dir = os.path.dirname(__file__)
_root_dir = os.path.dirname(_cur_dir)


def verify_telegram_urls(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    cookies = {
        # 在这里添加需要的Cookies
    }
    responseContent = {}
    success_count = 0

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

        # 尝试从主要的标签中获取内容
        page_title = get_content_by_class(soup, 'tgme_page_title') or get_content_from_alternative(soup,
                                                                                                   'tgme_channel_info_header_title')
        page_extra = get_content_by_class(soup, 'tgme_page_extra') or get_content_from_alternative(soup,
                                                                                                   'tgme_channel_info_counters')
        page_description = get_content_by_class(soup, 'tgme_page_description') or get_content_from_alternative(soup,
                                                                                                               'tgme_channel_info_description')
        page_action = get_content_by_class(soup, 'tgme_page_action')
        action_button = soup.find('a', class_='tgme_action_button_new shine')
        action_button = action_button.text.strip() if action_button else None
        page_context_link_wrap = get_content_by_class(soup, 'tgme_page_context_link_wrap')
        page_additional = get_content_by_class(soup, 'tgme_page_additional')
        # msgError = get_content_by_class(soup, 'tgme_widget_message_error') or get_content_from_alternative(soup,
        #                                                                                                    'tgme_widget_message_error')
        # if msgError is not None and msgError != '':
        #     page_description = msgError

        urlType = "Unknown"
        if page_extra:
            if page_extra.__contains__('member'):
                urlType = "Group"
            elif page_extra.__contains__("subscriber"):
                urlType = "Channel"

        if page_action == 'Send Message':
            page_action = 'Send Message'
        responseContent[url] = {
            "urlType": urlType,
            "page_title": page_title,
            "page_extra": page_extra,
            "page_description": page_description,
            "page_action": page_action,
            "action_button": action_button,
            "page_context_link_wrap": page_context_link_wrap,
            "page_additional": page_additional
        }
        success_count += 1
    except requests.RequestException as e:
        responseContent[url] = {"error": str(e)}

    return responseContent, success_count


def save_to_sqlite(data, batch_code):
    conn = sqlite3.connect(_root_dir + '/telegram_data.db')
    c = conn.cursor()

    # # 创建表（如果不存在）
    # c.execute('''
    #     CREATE TABLE IF NOT EXISTS valid_group_data (
    #         url TEXT PRIMARY KEY,
    #         page_title TEXT,
    #         page_extra TEXT,
    #         members_subscribers Integer,
    #         url_type TEXT,
    #         page_description TEXT,
    #         page_description_language TEXT,
    #         page_action TEXT,
    #         action_button TEXT,
    #         page_context_link_wrap TEXT,
    #         page_additional TEXT,
    #         error TEXT,
    #         created_at TEXT,
    #         updated_at TEXT,
    #         batch_code TEXT
    #     )
    # ''')

    # 获取当前UTC时间
    current_utc_time = datetime.now(timezone.utc)
    # 将UTC时间加上7小时
    time_plus_7_hours = current_utc_time + timedelta(hours=7)
    # 转换为ISO格式字符串
    time_format = time_plus_7_hours.strftime('%Y-%m-%d %H:%M:%S')

    for url, content in data.items():
        content = content.get(url)
        if content:
            # 检查 URL 是否已存在
            c.execute('SELECT COUNT(1) FROM valid_group_data WHERE url = ?', (url,))
            exists = c.fetchone()[0] > 0

            if exists:
                # 更新现有记录的 updated_at
                c.execute('''
                    UPDATE valid_group_data
                    SET
                        page_title = ?,
                        page_extra = ?,
                        url_type = ?,
                        page_description = ?,
                        page_description_language = ?,
                        page_action = ?,
                        action_button = ?,
                        page_context_link_wrap = ?,
                        page_additional = ?,
                        error = ?,
                        updated_at = ?
                    WHERE url = ?
                ''', (
                    url,
                    content.get("page_title"),
                    content.get("page_extra"),
                    content.get("url_type"),
                    content.get("page_description"),
                    content.get("page_description_language"),
                    content.get("page_action"),
                    content.get("action_button"),
                    content.get("page_context_link_wrap"),
                    content.get("page_additional"),
                    content.get("error"),
                    time_format
                ))
            else:
                # 插入新记录
                c.execute('''
                    INSERT INTO  valid_group_data (
                        url, 
                        page_title, 
                        page_extra, 
                        url_type, 
                        page_description,
                        page_description_language, page_action, action_button, page_context_link_wrap, page_additional, error, created_at,batch_code
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?)
                ''', (
                    url,
                    content.get("page_title"),
                    content.get("page_extra"),
                    content.get("url_type"),
                    content.get("page_description"),
                    content.get("page_description_language"),
                    content.get("page_action"),
                    content.get("action_button"),
                    content.get("page_context_link_wrap"),
                    content.get("page_additional"),
                    content.get("error"),
                    time_format,
                    batch_code
                ))

    conn.commit()
    conn.close()
    return


def convert_subscribers(value):
    """将带有 'K' 的字符串转换为整数"""
    try:
        value = value.strip()
        value = value.replace(" ", "")
        if 'K' in value:
            numeric_part = value.replace('K', '')
            return int(float(numeric_part) * 1000)
        elif 'M' in value:
            numeric_part = value.replace('M', '')
            return int(float(numeric_part) * 1000000)
        elif 'B' in value:
            numeric_part = value.replace('B', '')
            return int(float(numeric_part) * 1000000000)
        elif 'no' in value:
            return 0
        else:
            return int(value)
    except:
        return 0


def update_telegram_data():
    """更新数据库中的数据"""
    conn = sqlite3.connect(_root_dir + '/telegram_data.db')
    c = conn.cursor()

    # 从数据库中获取原始数据
    c.execute('SELECT rowid, page_extra FROM valid_group_data')
    rows = c.fetchall()

    # 更新每一行的数据
    for row in rows:
        rowid, page_extra = row
        if page_extra:
            converted_number = 0
            url_type = None
            number_str = ["0"]
            # 提取数量并进行转换
            if 'members' in page_extra:
                number_str = page_extra.split('members')
                url_type = "Group"
            elif 'member' in page_extra:
                number_str = page_extra.split('member')
                url_type = "Group"
            elif 'subscribers' in page_extra:
                number_str = page_extra.split('subscribers')
                url_type = "Channel"
            elif 'subscriber' in page_extra:
                number_str = page_extra.split('subscriber')
                url_type = "Channel"
            elif page_extra.startswith('@'):
                url_type = "User"
            _number_str = number_str[0]
            converted_number = convert_subscribers(_number_str)

            # 更新数据库
            c.execute('UPDATE valid_group_data SET members_subscribers = ?,url_type=? WHERE rowid = ?',
                      (converted_number, url_type, rowid))

    conn.commit()
    conn.close()


def getWebInfo(urlsAll, batchNo):
    # 创建表（如果不存在）
    conn = sqlite3.connect(_root_dir + '/telegram_data.db')
    c = conn.cursor()
    c.execute('''
               CREATE TABLE IF NOT EXISTS valid_group_data (
                   url TEXT PRIMARY KEY,
                   page_title TEXT,
                   page_extra TEXT,
                   members_subscribers Integer,
                   url_type TEXT,
                   page_description TEXT,
                   page_description_language TEXT,
                   page_action TEXT,
                   action_button TEXT,
                   page_context_link_wrap TEXT,
                   page_additional TEXT,
                   error TEXT,
                   created_at TEXT,
                   updated_at TEXT,
                   batch_code TEXT
               )
           ''')
    # 从数据库中删除旧数据
    c.execute('delete FROM valid_group_data')
    conn.commit()
    conn.close()

    urls = list(set(urlsAll))
    total_urls = len(urls)
    if not urls:
        print("No URLs found in the Excel file.")
        return
    else:
        print(f"去重 URLs read from Excel: {total_urls}")

    # 验证Telegram链接,循环从urls取20条数据进行验证,并保存到response
    batch_size = 20
    success_count = 0
    batch_code = uu.uuid4().hex

    start_time1 = datetime.now()
    with ThreadPoolExecutor(max_workers=2) as executor:
        for i in range(0, total_urls, batch_size):
            start_time = datetime.now()

            batch_urls = urls[i:i + batch_size]
            response_data = {}

            futures = {executor.submit(verify_telegram_urls, url): url for url in batch_urls}

            for future in as_completed(futures):
                url = futures[future]
                try:
                    _data, _ = future.result()
                    # print(f'data:{_data}')
                    response_data[url] = _data
                    if "error" not in _data:
                        success_count += 1
                except Exception as exc:
                    response_data[url] = {"error": str(exc)}

            save_to_sqlite(response_data, batch_code)
            print(f"Processed batch {i // batch_size + 1} with {success_count} successful URLs")

            end_time = datetime.now()
            duration = end_time - start_time
            print(f"Successfully retrieved URLs: {success_count},batchNo:{batchNo}")
            print(f"{end_time},Time taken: {duration}")
            # time.sleep(1)

            # _tc = success_count / 10000
            # if _tc.is_integer():
            #     time.sleep(1)
            # print(f"Sleeping for 20 seconds")
            # else:
            #     time.sleep(1)

    end_time = datetime.now()
    duration = end_time - start_time1
    print(f"Successfully retrieved URLs: {success_count}")
    print(f"{end_time},Time taken: {duration}")

    update_telegram_data()
    return


def query_telegram_data():
    conn = sqlite3.connect(_root_dir + '/telegram_data.db')
    c = conn.cursor()
    c.execute('''
        SELECT url
            , case when url_type= 'Group' then 0 when url_type= 'Channel' then 1 else null end as linkType
            , page_title as name
            , page_description as description
            , members_subscribers as userNum
        FROM valid_group_data 
        WHERE page_title is not null and page_title!='' and url_type in('Group','Channel')
    ''')
    data_link = c.fetchall()

    conn.close()
    return data_link


def query_delete_data():
    conn = sqlite3.connect(_root_dir + '/telegram_data.db')
    c = conn.cursor()
    c.execute('''
        SELECT url
        FROM valid_group_data 
        WHERE (page_title is null or url_type='User') and (page_action in('View Post','Send Message','Start Bot','Join Group') 
        or page_description like 'If you have Telegram, you can view postsby%'
        or (page_description is null and page_action is null and action_button is null and error is null))
    ''')
    data_link = c.fetchall()

    conn.close()
    return data_link


def updateGroup():
    # url = "http://localhost:8080/cms/test/supplyGroupInfo"
    url = "http://a5b684db7f9d746b18f60bbe60781a74-8b898802e7cba134.elb.ap-southeast-1.amazonaws.com:8800/cms/content/supplyGroupInfo"
    headers = {
        "appKey": "90b0e5c2eb0d11eeba8ee49d6628936c",
        "Content-Type": "application/json"
    }
    # 查询数据
    data_link = query_telegram_data()
    data_del = query_delete_data()
    row_count = len(data_del)
    # print(f"Found {row_count} rows in the database.")
    data_list = []
    count = 0
    if row_count > 0:
        for item in data_del:
            count = count + 1
            data = {
                "url": item[0],
                "status": 3
            }
            data_list.append(data)
            if len(data_list) >= 200:
                try:
                    response = requests.post(url, headers=headers, json=data_list)
                    print(f"{datetime.now()},下架count:{count},{response.text}")
                except Exception as e:
                    print(e)
                data_list = []
        if len(data_list) > 0:
            try:
                response = requests.post(url, headers=headers, json=data_list)
                print(f"{datetime.now()},下架count:{count},{response.text}")
            except Exception as e:
                print(e)
            data_list = []

    row_count = len(data_link)
    if row_count > 0:
        size = 0
        count = 0
        data_list = []
        for item in data_link:
            size = size + 1
            count = count + 1
            data = {
                "url": item[0],
                "linkType": item[1],
                "status": 1
            }
            json = {
                "name": item[2],
                "description": item[3],
                "userNum": item[4],
            }
            data["extendJson"] = json
            data_list.append(data)
            if size >= 200:
                try:
                    response = requests.post(url, headers=headers, json=data_list)
                    print(f"{datetime.now()},count:{count},{response.text}")
                except Exception as e:
                    print(e)
                data_list = []
                size = 0
        if size > 0:
            try:
                response = requests.post(url, headers=headers, json=data_list)
                print(f"{datetime.now()},count:{count},{response.text}")
            except Exception as e:
                print(e)

        url = "http://a7a4a32172a574102a42076ffca608eb-7ff1940d50a42959.elb.ap-southeast-1.amazonaws.com:8800/rule/test/batchPullData"
        json_param = {
            "functionNames": "cms_group_rule,cms_group_rule_en",
            "isRun": 1
        }
        try:
            requests.post(url, headers=headers, json=json_param)
        except Exception as e:
            print(e)

        print(f"{datetime.now()},总下架count:{len(data_del)}")
        print(f"{datetime.now()},Data has been saved,cnt:{len(data_link)}")
        print(f"{datetime.now()},Data has been run,cnt:{row_count}")
    else:
        print("No data found in the database.")


# def testData():
#     url = "http://a901c69ff2d4c4b9bb678f3ebc6ea4c1-a52e4692b9f44640.elb.ap-southeast-1.amazonaws.com:8800/cms/test/publisher"
#     headers = {
#         "appKey": "90b0e5c2eb0d11eeba8ee49d6628936c",
#         "Content-Type": "application/json"
#     }
#
#     file_path = os.path.join(_cur_dir, 'f1_group_url.csv')
#     urls = []
#     try:
#         df = pd.read_csv(file_path, header=None)
#         urls = df.iloc[:, 0].tolist()  # 读取第一列的URL
#     except Exception as e:
#         print(f"Error reading URLs from Excel: {e}")
#     count = 0
#     for item in urls:
#         if len(item.split("/")) > 4:
#             continue
#         json_param = {
#             "isExt": 0,
#             "queue": "cms_group_supply_queue",
#             "data": {"url": item}
#         }
#         rp = requests.post(url, headers=headers, json=json_param)
#         count = count + 1
#         print(count)

def csv_import():
    # groupRabbit.handApiCsv()
    file_path = os.path.join(_cur_dir, 'f1_group_url.csv')
    urls = []
    try:
        df = pd.read_csv(file_path, header=None)
        urls = df.iloc[:, 0].tolist()  # 读取第一列的URL
    except Exception as e:
        print(f"Error reading URLs from Excel: {e}")
    getWebInfo(urls, 1)
    updateGroup()


def getData(size):
    url = "http://a5b684db7f9d746b18f60bbe60781a74-8b898802e7cba134.elb.ap-southeast-1.amazonaws.com:8800/cms/content/getSupplyGroups"
    headers = {
        "appKey": "90b0e5c2eb0d11eeba8ee49d6628936c",
        "Content-Type": "application/json"
    }
    response = requests.post(url, headers=headers, json={"size": size})
    if response.status_code != 200:
        print({"error": f"Status code: {response.status_code}"})
        return []
    lst_data = json.loads(response.content)["data"]
    urls = []
    for data in lst_data:
        urls.append(data["url"])
    return urls


def api_import():
    i = 0
    count = 0
    while i < 1000:
        i = i + 1
        urls = getData(500)
        if len(urls) == 0:
            break
        count = count + len(urls)
        getWebInfo(urls, i)
        updateGroup()
        print(f"group,count：{count}")
    print(f"group api_import end,count:{count}")
    return count


if __name__ == "__main__":
    # api_import()
    csv_import()

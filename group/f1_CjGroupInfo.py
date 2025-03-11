import ast
import json
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

def query_telegram_data():
    root_dir = os.path.dirname(os.path.dirname(__file__))
    conn = sqlite3.connect(root_dir + '/telegram_data.db')
    c = conn.cursor()
    c.execute('''
        SELECT url, case when url_type= 'Group' then 0 when url_type= 'Channel' then 1 else null end as linkType
            ,case when page_description_language= 'zh' then 1 when page_description_language= 'en' then 2 else 0 end as langType
            ,page_title as name, page_description as description, members_subscribers as userNum
        FROM telegram_data 
        WHERE page_title is not null and page_title!='' and url_type in('Group','Channel') and members_subscribers>100
    ''')
    data_link = c.fetchall()

    conn.close()
    return data_link

def query_telegram_data2():
    root_dir = os.path.dirname(os.path.dirname(__file__))
    conn = sqlite3.connect(root_dir + '/telegram_data.db')
    c = conn.cursor()
    c.execute('''
        SELECT url, case when url_type= 'Group' then 0 when url_type= 'Channel' then 1 else null end as linkType
            ,case when page_description_language= 'zh' then 1 when page_description_language= 'en' then 2 else 0 end as langType
            ,page_title as name, page_description as description, members_subscribers as userNum
        FROM telegram_data 
        WHERE page_title is not null and page_title!='' and url_type in('Group','Channel') 
    ''')
    data_link = c.fetchall()

    conn.close()
    return data_link

def importGroup(source):
    data_link = query_telegram_data()
    run(data_link, source)

def importGroup2(source):
    # 查询数据 1中文 2英文 0其他 ，没有人员限制
    data_link = query_telegram_data2()
    run(data_link, source)

def run(data_link, source):
    row_count = len(data_link)
    start_time = datetime.now()
    print(f"{datetime.now()},Found {row_count} rows in the database.")
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
                "url": item[0],
                "linkType": item[1],
                "langType": item[2],
                "source": source
            }
            json = {
                "name": item[3],
                "description": item[4],
                "userNum": item[5]
            }
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
            "functionNames": "cms_group_rule",
            "isRun": 1
        }
        requests.post(url, headers=headers, json=json_param)
        print(f"Data has been run,count:{row_count},start:{start_time},end:{datetime.now()},")
    else:
        print("No data found in the database.")

def detect_language(text):
    total_cnt = len(text)
    if total_cnt < 10:
        return 'unknown'

    try:
        lang, _ = langid.classify(text)
        # lang = 'zh-cn' if lang == 'zh' else lang
        return lang
        # return detect(text)
    except LangDetectException:
        return "unknown"

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
        start_time = time.time()
        response = requests.get(url, headers=headers, cookies=cookies, timeout=10)
        elapsed_time = time.time() - start_time
        # print(f"{elapsed_time}")
        if elapsed_time < 0.7:
            time.sleep(0.7 - elapsed_time)

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
        if page_extra:
            if page_extra.__contains__('members'):
                urlType = "Group"
            elif page_extra.startswith("@"):
                urlType = "User"
            elif page_extra.__contains__("subscribers"):
                urlType = "Channel"
        else:
            urlType = "Unknown"  # 这里需要进一步处理逻辑以确定类型

        page_description = get_content_by_class(soup, 'tgme_page_description') or get_content_from_alternative(soup,
                                                                                                               'tgme_channel_info_description')
        page_action = get_content_by_class(soup, 'tgme_page_action')
        action_button = soup.find('a', class_='tgme_action_button_new shine')
        action_button = action_button.text.strip() if action_button else None
        page_context_link_wrap = get_content_by_class(soup, 'tgme_page_context_link_wrap')
        page_additional = get_content_by_class(soup, 'tgme_page_additional')

        description_language = "" if page_description is None else page_description
        description_language += "" if page_title is None else page_title
        description_language = detect_language(description_language)

        responseContent[url] = {
            "page_title": page_title,
            "page_extra": page_extra,
            "url_type": urlType,
            "page_description": page_description,
            "page_description_language": description_language,
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
    root_dir = os.path.dirname(os.path.dirname(__file__))
    conn = sqlite3.connect(root_dir + '/telegram_data.db')
    c = conn.cursor()

    # 创建表（如果不存在）
    c.execute('''
        CREATE TABLE IF NOT EXISTS telegram_data (
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
            c.execute('SELECT COUNT(1) FROM telegram_data WHERE url = ?', (url,))
            exists = c.fetchone()[0] > 0

            if exists:
                # 更新现有记录的 updated_at
                c.execute('''
                    UPDATE telegram_data
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
                    INSERT INTO telegram_data (
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
                c.execute('''
        UPDATE telegram_data
        SET members_subscribers = (
            CASE
                WHEN page_extra LIKE '%members%' THEN
                    CAST(
                        TRIM(
                            REPLACE(
                                SUBSTR(
                                    page_extra,
                                    1,
                                    INSTR(page_extra, ' members') - 1
                                ),
                                ' ',
                                ''
                            )
                        ) AS INTEGER
                    )
                WHEN page_extra LIKE '%subscribers%' THEN
                    CAST(
                        TRIM(
                            REPLACE(
                                SUBSTR(
                                    page_extra,
                                    1,
                                    INSTR(page_extra, ' subscribers') - 1
                                ),
                                ' ',
                                ''
                            )
                        ) AS INTEGER
                    )
                  WHEN page_extra = '1 subscriber' THEN  1 
                    WHEN page_extra = '1 member' THEN  1 
                ELSE NULL
            END
        ) 
        WHERE url = ?;
    ''', (url,))

    conn.commit()
    conn.close()
    return

def insertPromot(langType, promoterId, promoterName, startTime, batchNo, urls):
    # url = "http://localhost:8080/cms/content/importPromot"
    url = "http://a5b684db7f9d746b18f60bbe60781a74-8b898802e7cba134.elb.ap-southeast-1.amazonaws.com:8800/cms/content/importPromot"
    headers = {
        "appKey": "90b0e5c2eb0d11eeba8ee49d6628936c",
        "Content-Type": "application/json"
    }
    size = 0
    count = 0
    data_list = []
    for item in urls:
        size = size + 1
        count = count + 1
        # 正则
        data = {
            "promoter_id": promoterId,
            "promoter_name": promoterName,
            "url": re.sub("[/]+$", "", item.strip()),
            "lang_type": langType,
            "status": 0,
            "create_time": startTime,
            "batch_no": batchNo
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
    print(f"{datetime.now()},promot data has been saved")

def updatePromot(startTime):
    url = "http://a5b684db7f9d746b18f60bbe60781a74-8b898802e7cba134.elb.ap-southeast-1.amazonaws.com:8800/cms/content/execPromotProc"
    headers = {
        "appKey": "90b0e5c2eb0d11eeba8ee49d6628936c",
        "Content-Type": "application/json"
    }
    endTime = datetime.now(pytz.timezone('Asia/Shanghai')).strftime("%Y-%m-%d %H:%M:%S")
    print(f"promot startTime:{startTime}, endTime:{endTime}")
    time.sleep(60 * 30)
    response = requests.post(url, headers=headers, json={"startTime": startTime, "endTime": endTime})
    print(f"{datetime.now()},promot结果:{response.text}")

def getWebInfo(urlsAll):
    # 从数据库中删除旧数据
    root_dir = os.path.dirname(os.path.dirname(__file__))
    conn = sqlite3.connect(root_dir + '/telegram_data.db')
    c = conn.cursor()
    c.execute('delete FROM telegram_data')
    conn.commit()
    conn.close()

    urls = list(set(urlsAll))
    total_urls = len(urls)
    if not urls:
        print("no urls.")
        return
    # else:
    #     print(f"去重 URLs read from Excel: {total_urls}")

    # vpn_test_url =urls[0]
    # vpn_results = test_vpn_lines(vpn_test_url)
    # print(f"VPN geturl results: {vpn_results}")

    # 验证Telegram链接,循环从urls取20条数据进行验证,并保存到response
    batch_size = 20
    success_count = 0
    batch_code = uu.uuid4().hex

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
            # print(f"Processed batch {i // batch_size + 1} with {success_count} successful URLs")

            end_time = datetime.now()
            duration = end_time - start_time
            print(
                f"{end_time},Processed batch {i // batch_size + 1},Successfully retrieved URLs: {success_count},Time taken: {duration}")
            # time.sleep(1)

            # _tc = success_count / 10000
            # if _tc.is_integer():
            #     time.sleep(20)
            #     print(f"Sleeping for 20 seconds")
            # else:
            #     time.sleep(2)

    update_telegram_data()
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
    root_dir = os.path.dirname(os.path.dirname(__file__))
    conn = sqlite3.connect(root_dir + '/telegram_data.db')
    c = conn.cursor()

    # 从数据库中获取原始数据
    c.execute('SELECT rowid, page_extra FROM telegram_data')
    rows = c.fetchall()

    # 更新每一行的数据
    for row in rows:
        rowid, page_extra = row
        if page_extra:
            if 'members' in page_extra or 'subscribers' in page_extra:
                # 提取数量并进行转换
                number_str = page_extra.split('members')
                if 'subscribers' in page_extra:
                    number_str = page_extra.split('subscribers')
                _number_str = number_str[0]
                # for i, word in enumerate(number_str):
                #     if word.isdigit():
                #         _number_str += word
                #         continue
                #     else:
                #         break

                converted_number = convert_subscribers(_number_str)
                # 更新数据库
                c.execute('UPDATE telegram_data SET members_subscribers = ? WHERE rowid = ?', (converted_number, rowid))

    conn.commit()
    conn.close()

def handUrls(urls):
    total_urls = len(urls)
    urlsAll = []
    for item in urls:
        url = str(item).strip();
        url = re.sub("[/]+$", "", url)
        url = url.split("?")[0]
        url = url.replace("www.t.me", "t.me")
        url = url.replace("t.me/s/", "t.me/")
        url = url.replace("http:", "https:")
        url = url.replace(" ", "")
        url = url.replace("/t.me/joinchat/", "/t.me/")
        # url = url.replace("/t.me/share/", "/t.me/")
        # if 'https://t.me/' not in url:
        #     print(f"不识别的链接: {url}")
        #     urlsAll = []
        #     return
        if len(url.split("/")) > 3:
            url = "https://t.me/" + url.split("/")[3]
        if '+' in url or not url.startswith("https://t.me/"):
            continue
        urlsAll.append(url)
    urlsAll = list(set(urlsAll))
    print(f"Total URLs read from Excel: {total_urls},valid:{len(urlsAll)}")
    return urlsAll

def excel_import(batchNo, langType, source):
    _dir = os.path.dirname(__file__)
    file_path = os.path.join(_dir, 'f1_Cj.xlsx')
    excel_file = pd.ExcelFile(file_path)
    # 获取所有工作表的名称
    sheet_names = excel_file.sheet_names
    # 遍历每个工作表并读取数据
    arrUser = []
    for sheet_name in sheet_names:
        # 读取工作表数据
        df = excel_file.parse(sheet_name, header=None)
        if '总' in sheet_name or '合计' in sheet_name:
            for index, row in df.iterrows():
                if str(row[1]) == "nan" or row[1] == None:
                    continue
                jsonObj = {
                    "userId": str(row[0]).split('.')[0],
                    "userName": row[1]
                }
                arrUser.append(jsonObj)
        else:
            urls = df.iloc[:, 0].tolist()  # 读取第一列的URL
            urls = handUrls(urls)
            getWebInfo(urls)
            user = None
            for item in arrUser:
                if str(item["userName"]) in sheet_name:
                    user = item
                    break
            startTime = datetime.now(pytz.timezone('Asia/Shanghai')).strftime("%Y-%m-%d %H:%M:%S")
            promoterId = user["userId"] if user else ""
            promoterName = user["userName"] if user else sheet_name
            insertPromot(langType, promoterId, promoterName, startTime, batchNo, urls)
            importGroup(source)
            t = threading.Thread(target=updatePromot, args=(startTime,))
            t.daemon = False
            t.start()

def csv_import(source):
    _dir = os.path.dirname(__file__)
    file_path = os.path.join(_dir, 'f1_Cj_url.csv')
    urls = []
    try:
        df = pd.read_csv(file_path, header=None)
        urls = df.iloc[:, 0].tolist()  # 读取第一列的URL
    except Exception as e:
        print(f"Error reading URLs from Excel: {e}")

    urls = handUrls(urls)
    getWebInfo(urls)
    importGroup2(source)
    # updateGroup()
    # Asia/Bangkok
    # startTime = datetime.now(pytz.timezone('Asia/Shanghai')).strftime("%Y-%m-%d %H:%M:%S")
    # batchNo = "20240824"
    # promoterId = "7361296860"
    # promoterName = "爱意随风"
    # insertPromot(2, promoterId, promoterName, startTime, batchNo, urls)
    # importGroup()
    # updatePromot(startTime)

def dir_import(source):
    _dir = os.path.dirname(__file__) + "/url"
    files = os.listdir(_dir)
    for item in files:
        if '.txt' in item:
            urls = []
            try:
                with open(_dir + '/' + item, 'r', encoding='utf-8') as file:
                    content = file.read()
                    urls = ast.literal_eval(content)
                    urls = handUrls(urls)
                    getWebInfo(urls)
                    importGroup2(source)
            except Exception as e:
                print(f"Error reading URLs from Excel: {e}")

def sendMsg(msg):
    API_ID = '26534384'
    API_HASH = '8c32b586b4c83b04b9ab7d50c6464907'
    BOT_TOKEN = '7931792484:AAEX1QM7483FqCpcRvwFBEdbr2_Hb6uWp5s'
    client = Client('myapp', API_ID, API_HASH, bot_token=BOT_TOKEN)
    with client:
        client.send_message("cjaniu", msg)

if __name__ == "__main__":
    # 创建表（如果不存在）
    root_dir = os.path.dirname(os.path.dirname(__file__))
    conn = sqlite3.connect(root_dir + '/telegram_data.db')
    c = conn.cursor()
    c.execute('''
            CREATE TABLE IF NOT EXISTS telegram_data (
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
    conn.commit()
    conn.close()

    dir_import(5)
    # csv_import(5)
    # excel_import("2024101802", 2,6)
    # excel_import(datetime.now().strftime("%Y%m%d"), 2, 6)
    print(f"{datetime.now()},wait...")

    response = requests.get('https://api.ipify.org', timeout=5)
    sendMsg(f"{datetime.now()},[ **valid end** ],ip:{response.text}")
    # parse_mode = HTML parse_mode = Markdown

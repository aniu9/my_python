import os
import subprocess
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import time

try:
    subprocess.run(['pip', 'install', 'bs4'], check=True)
    subprocess.run(['pip', 'install', 'requests'], check=True)
    subprocess.run(['pip', 'install', 'pyrogram'], check=True)
    print(f'Package installed successfully.')
except subprocess.CalledProcessError as e:
    print(f'Error installing package: {e}')

import requests
from bs4 import BeautifulSoup
from pyrogram import Client

# _cur_dir = os.path.dirname(__file__)
# _root_dir = os.path.dirname(_cur_dir)


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
            "url_type": urlType,
            "page_title": page_title,
            "page_extra": page_extra,
            "page_description": page_description,
            "page_action": page_action,
            "action_button": action_button,
            "page_context_link_wrap": page_context_link_wrap,
            "page_additional": page_additional
        }
        success_count += 1
    except Exception as e:
        responseContent[url] = {"error": str(e)}

    return responseContent, success_count

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

def update_telegram_data(content):
    # 更新数据
    page_extra = content.get("page_extra")
    if page_extra:
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
        content["members_subscribers"] = converted_number
        content["url_type"] = url_type
    return content

def getWebInfo(urlsAll, batchNo):
    lstData = []
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

            for url, content in response_data.items():
                data = update_telegram_data(content.get(url))
                data["url"] = url
                lstData.append(data)

            print(f"Processed batch {i} with {success_count} successful URLs")
            end_time = datetime.now()
            duration = end_time - start_time
            print(f"Successfully retrieved URLs: {success_count},batchNo:{batchNo}")
            print(f"{end_time},Time taken: {duration}")

    end_time = datetime.now()
    duration = end_time - start_time1
    print(f"Successfully retrieved URLs: {success_count}")
    print(f"{end_time},Time taken: {duration}")
    return lstData

def query_telegram_data(lstData):
    result = []
    for item in lstData:
        if item.get("page_title") is not None and item.get("page_title") != "" and (
                item.get("url_type") == "Group" or item.get("url_type") == "Channel"):
            obj = {
                "url": item.get("url"),
                "linkType": 0 if item.get("url_type") == "Group" else 1,
                "page_title": item.get("page_title"),
                "description": item.get("page_description"),
                "userNum": item.get("members_subscribers")
            }
            result.append(obj)
    return result

def query_delete_data(lstData):
    result = []
    for item in lstData:
        page_action = "" if item.get("page_action") is None else item.get("page_action")
        description = "" if item.get("page_description") is None else item.get("page_description")
        if (item.get("page_title") is None or item.get("url_type") == "User") and (
                page_action in ['View Post', 'Send Message', 'Start Bot', 'Join Group'] or
                "If you have Telegram, you can view postsby" in description or
                (description == "" and item.get("page_action") == "" and
                 item.get("action_button") is None and item.get("error") is None)):
            obj = {
                "url": item.get("url")
            }
            result.append(obj)
    return result

def updateGroup(lstData):
    # url = "http://localhost:8080/cms/test/supplyGroupInfo"
    url = "cms/content/supplyGroupInfo"
    headers = {
        "appKey": "90b0e5c2eb0d11eeba8ee49d6628936c",
        "Content-Type": "application/json"
    }
    # 查询数据
    data_link = query_telegram_data(lstData)
    data_del = query_delete_data(lstData)
    row_count = len(data_del)
    # print(f"Found {row_count} rows in the database.")
    data_list = []
    count = 0
    if row_count > 0:
        for item in data_del:
            count = count + 1
            data = {
                "url": item["url"],
                "status": 3
            }
            data_list.append(data)
            # if len(data_list) >= 200:
            #     response = requests.post(url, headers=headers, json=data_list)
            #     print(f"{datetime.now()},下架count:{count},{response.text}")
            #     data_list = []
        if len(data_list) > 0:
            # response = requests.post(url, headers=headers, json=data_list)
            response = reqAPi(url, data_list)
            print(f"{datetime.now()},下架count:{count}")
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
                "url": item["url"],
                "linkType": item["linkType"],
                "status": 1
            }
            json = {
                "name": item["page_title"],
                "description": item["description"],
                "userNum": item["userNum"],
            }
            data["extendJson"] = json
            data_list.append(data)
            # if size >= 200:
            #     response = requests.post(url, headers=headers, json=data_list)
            #     print(f"{datetime.now()},count:{count}")
            #     print(response.text)
            #     data_list = []
            #     size = 0
        if size > 0:
            # requests.post(url, headers=headers, json=data_list)
            response = reqAPi(url, data_list)

        # url = "http://a7a4a32172a574102a42076ffca608eb-7ff1940d50a42959.elb.ap-southeast-1.amazonaws.com:8800/rule/test/batchPullData"
        # json_param = {
        #     "functionNames": "cms_group_rule,cms_group_rule_en",
        #     "isRun": 1
        # }
        # requests.post(url, headers=headers, json=json_param)

        print(f"{datetime.now()},总下架count:{len(data_del)}")
        print(f"{datetime.now()},总更新count:{len(data_link)}")
        # print(f"{datetime.now()},Data has been run,cnt:{row_count}")
    else:
        print("No data found in the database.")

import hashlib
import json

def md5_hash(input_str):
    md5 = hashlib.md5()
    # Ensure the string is encoded to bytes before hashing
    md5.update(input_str.encode('utf-8'))
    return md5.hexdigest()

def get_secret(app_key):
    # Mimicking the SignStringUtils.left and right functions from Java
    hash_key = md5_hash("key" + app_key)
    return hash_key[:15][-10:]

def sign(app_key, timestamp, request_body):
    try:
        secret = get_secret(app_key)
        sb_sign = app_key + secret + str(timestamp) + request_body
        # Hash the concatenated string and convert to uppercase
        sign_result = md5_hash(sb_sign).upper()
        return sign_result
    except Exception as e:
        print(f"生成签名异常: {e}")
        return None

def reqAPi(url, data):
    url = url.strip("/")
    headers = {'Content-Type': 'application/json'}
    url = "http://a901c69ff2d4c4b9bb678f3ebc6ea4c1-a52e4692b9f44640.elb.ap-southeast-1.amazonaws.com:8800/" + url
    # url = "http://a276b8d3ca3a14befa1dc6335eaa47ea-f83cb44aa303c283.elb.ap-southeast-1.amazonaws.com:8800/cms-service/" + url
    # now = datetime.now()
    # timestamp = int(now.timestamp() * 1000)
    # j = json.dumps(data)
    # SERVER_APP_KEY = "1"
    # s = sign(str(SERVER_APP_KEY), timestamp, j)
    # headers = {
    #     'Content-Type': 'application/json',
    #     "appkey": str(SERVER_APP_KEY),
    #     "timestamp": str(timestamp),
    #     "sign": s
    # }

    try:
        response = requests.post(url, headers=headers, json=data, timeout=30)
        if response.status_code != 200:
            print({"error": f"Status code: {response.status_code}"})
            return []
        result = json.loads(response.content)
        if result["code"] != 200:
            print(f"error: {result}")
            return []
        return result["data"]
    except:
        return []

def getData(size):
    url = "cms/content/getSupplyGroups"
    data = {"size": size}
    lst_data = reqAPi(url, data)
    urls = []
    for data in lst_data:
        urls.append(data["url"])
    # file_path = os.path.join(_cur_dir, 'f1_group_url.csv')
    # df = pd.read_csv(file_path, header=None)
    # urls = df.iloc[:, 0].tolist()
    return urls

def api_import(i, size):
    urls = getData(size)
    if len(urls) == 0:
        return 0
    lstData = getWebInfo(urls, i)
    updateGroup(lstData)
    return len(urls)

def api_import_msg(i, size):
    urls = get_data_msg(size)
    # urls=["https://t.me/qiqubaike/319567"]
    if len(urls) == 0:
        return 0
    lstData = getWebInfo(urls, i)
    update_msg(lstData)
    return len(urls)

def get_data_msg(size):
    url = "cms/content/getSupplyMsgs"
    data = {"size": size}
    lst_data = reqAPi(url, data)
    urls = []
    for data in lst_data:
        urls.append(data["url"])
    # file_path = os.path.join(_cur_dir, 'f2_msg_url.csv')
    # df = pd.read_csv(file_path, header=None)
    # urls = df.iloc[:, 0].tolist()  # 读取第一列的URL
    return urls

def update_msg(lstData):
    # url = "http://localhost:8080/cms/content/supplyMsgInfo"
    url = "cms/content/supplyMsgInfo"
    headers = {
        "appKey": "90b0e5c2eb0d11eeba8ee49d6628936c",
        "Content-Type": "application/json"
    }
    # 查询数据
    data_del = query_delete_data_msg(lstData)
    data_link = query_telegram_data_msg(lstData)
    row_count = len(data_del)
    data_list = []
    count = 0
    if row_count > 0:
        for item in data_del:
            count = count + 1
            data = {
                "url": item["url"],
                "status": 3
            }
            data_list.append(data)
            # if len(data_list) >= 200:
            #     response = requests.post(url, headers=headers, json=data_list)
            #     print(f"{datetime.now()},下架count:{count},{response.text}")
            #     data_list = []
        if len(data_list) > 0:
            # response = requests.post(url, headers=headers, json=data_list)
            response = reqAPi(url, data_list)
            print(f"{datetime.now()},下架count:{count}")
            data_list = []
    count = 0
    row_count = len(data_link)
    if row_count > 0:
        for item in data_link:
            count = count + 1
            data = {
                "url": item["url"],
                "status": 1
            }
            data_list.append(data)
            # if len(data_list) >= 200:
            #     response = requests.post(url, headers=headers, json=data_list)
            #     print(f"{datetime.now()},更新count:{count},{response.text}")
            #     data_list = []
        if len(data_list) > 0:
            # response = requests.post(url, headers=headers, json=data_list)
            response = reqAPi(url, data_list)
            print(f"{datetime.now()},更新count:{count}")
            data_list = []
    print(f"无效数据：{len(data_del)} 条.")
    print(f"有效数据：{len(data_link)} 条.")

def query_telegram_data_msg(lstData):
    result = []
    for item in lstData:
        if len(item.get("url").split("/")) == 5 and item.get("action_button") is not None and item.get(
                "action_button") in [
            'View In Group', 'View In Channel']:
            obj = {
                "url": item.get("url")
            }
            result.append(obj)
    return result

def query_delete_data_msg(lstData):
    result = []
    for item in lstData:
        if (len(item.get("url").split("/")) != 5 or (item.get("page_action") is not None
                                                     and item.get("page_action") in ['View Post', 'Send Message',
                                                                                     'Open App'])):
            obj = {
                "url": item.get("url")
            }
            result.append(obj)
    return result

def sendMsg(msg):
    API_ID = '26534384'
    API_HASH = '8c32b586b4c83b04b9ab7d50c6464907'
    BOT_TOKEN = '7931792484:AAEX1QM7483FqCpcRvwFBEdbr2_Hb6uWp5s'
    client = Client('myapp', API_ID, API_HASH, bot_token=BOT_TOKEN)
    with client:
        client.send_message("cjaniu", msg)

def getEmptyData(size):
    url = "cms/content/getEmptyLinks"
    data = {"pageSize": size}
    lst_data = reqAPi(url, data)
    urls = []
    for data in lst_data:
        urls.append(data["url"])
    # file_path = os.path.join(_cur_dir, 'f1_group_url.csv')
    # df = pd.read_csv(file_path, header=None)
    # urls = df.iloc[:, 0].tolist()
    return urls

def updateEmpty(lstData):
    # url = "http://localhost:8080/cms/test/supplyGroupInfo"
    url = "cms/content/collectGroupInfo"
    # 查询数据
    data_link = query_telegram_data(lstData)
    data_del = query_delete_data(lstData)
    row_count = len(data_del)
    # print(f"Found {row_count} rows in the database.")
    data_list = []
    count = 0
    if row_count > 0:
        for item in data_del:
            count = count + 1
            data = {
                "url": item["url"],
                "isValid": 0
            }
            data_list.append(data)
            # if len(data_list) >= 200:
            #     response = requests.post(url, headers=headers, json=data_list)
            #     print(f"{datetime.now()},下架count:{count},{response.text}")
            #     data_list = []
        if len(data_list) > 0:
            # response = requests.post(url, headers=headers, json=data_list)
            response = reqAPi(url, data_list)
            print(f"{datetime.now()},无效count:{count}")
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
                "url": item["url"],
                "linkType": item["linkType"],
                "isValid": 1,
                "title": item["page_title"],
                "description": item["description"],
                "userNum": item["userNum"]
            }
            data_list.append(data)
            # if size >= 200:
            #     response = requests.post(url, headers=headers, json=data_list)
            #     print(f"{datetime.now()},count:{count}")
            #     print(response.text)
            #     data_list = []
            #     size = 0
        if size > 0:
            # requests.post(url, headers=headers, json=data_list)
            response = reqAPi(url, data_list)

        # url = "http://a7a4a32172a574102a42076ffca608eb-7ff1940d50a42959.elb.ap-southeast-1.amazonaws.com:8800/rule/test/batchPullData"
        # json_param = {
        #     "functionNames": "cms_group_rule,cms_group_rule_en",
        #     "isRun": 1
        # }
        # requests.post(url, headers=headers, json=json_param)

        print(f"{datetime.now()},总无效count:{len(data_del)}")
        print(f"{datetime.now()},总导入count:{len(data_link)}")
        # print(f"{datetime.now()},Data has been run,cnt:{row_count}")
    else:
        print("No data found in the database.")

def empty_import(i, size):
    urls = getEmptyData(size)
    if len(urls) == 0:
        return 0
    lstData = getWebInfo(urls, i)
    updateEmpty(lstData)
    return len(urls)

def businessCode():
    emptyCnt = 0
    i = 0
    while i < 500:
        i = i + 1
        cnt = empty_import(i, 20)
        if cnt == 0:
            break
        else:
            emptyCnt = cnt + emptyCnt
    print(f"{datetime.now()},import end,emptyLink count:{emptyCnt}")
    if emptyCnt < 100:
        print(f"{datetime.now()},no data，sleep 10 minutes")
        time.sleep(600)
    else:
        response = requests.get('https://api.ipify.org', timeout=5)
        sendMsg(f"{datetime.now()},import end\nip:{response.text}, emptyLink count:{emptyCnt}")

if __name__ == "__main__":
    try:
        response = requests.get('https://api.ipify.org', timeout=5)
        sendMsg(f"{datetime.now()},import start\nip:{response.text}")
        while True:
            businessCode()
    except Exception as e:
        print(f"businessCode error: {e}")

    # file_path = os.path.join(os.path.dirname(__file__), 'f2_msg_url.csv')
    # df = pd.read_csv(file_path, header=None)
    # urls = df.iloc[:, 0].tolist()  # 读取第一列的URL
    # listData= getWebInfo(urls, 1)
    # print(len(query_delete_data_msg(listData)))

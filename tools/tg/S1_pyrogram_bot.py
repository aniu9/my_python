import asyncio
import hashlib
import json
from datetime import datetime, timedelta

import requests
from pyrogram import Client, errors

# 166 cobb923 阿牛
api_id = '26534384'
api_hash = '8c32b586b4c83b04b9ab7d50c6464907'
# cjss_my_bot 超级阿牛 chaojianiu
bot_token = '7781291686:AAH3L0XGwzkhtJvyeJzhC-Dy2D71NVu-a3k'
user_id = '5380352615'

def getBot():
    bot = Client('my_bot', api_id=api_id, api_hash=api_hash, bot_token=bot_token)
    return bot

def getApp():
    client = Client('my_app', api_id, api_hash)
    return client

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
    url = "http://a276b8d3ca3a14befa1dc6335eaa47ea-f83cb44aa303c283.elb.ap-southeast-1.amazonaws.com:8800" + url
    now = datetime.now()
    # 将 datetime 对象转换为时间戳
    timestamp = int(now.timestamp() * 1000)
    j = json.dumps(data)
    SERVER_APP_KEY = "1"
    s = sign(str(SERVER_APP_KEY), timestamp, j)
    headers = {
        'Content-Type': 'application/json',
        "appkey": str(SERVER_APP_KEY),
        "timestamp": str(timestamp),
        "sign": s
    }

    # url = "http://a5b684db7f9d746b18f60bbe60781a74-8b898802e7cba134.elb.ap-southeast-1.amazonaws.com:8800/cms/content/getSupplyGroups"
    # url = "http://a276b8d3ca3a14befa1dc6335eaa47ea-f83cb44aa303c283.elb.ap-southeast-1.amazonaws.com:8800/cms-service/cms/content/getSupplyGroups"
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
    url = "/cms-service/cms/content/getSupplyGroups"
    data = {"size": size}
    lst_data = reqAPi(url, data)
    urls = []
    for data in lst_data:
        urls.append(data["url"])
    return urls

async def get_me_info(client):
    async with client as cl:
        chat = await cl.get_me()
        print(chat)

async def get_group_info(chat_id):
    async with getBot() as bot:
        print(f"{datetime.now()}")
        try:
            chat = await bot.get_chat(chat_id)
            print(f"Chat ID: {chat.id}")
            print(f"Chat Title: {chat.title}")
            print(f"Chat Username: {chat.username}")
            print(f"Chat Type: {chat.type}")
            # print(chat)
        except errors.FloodWait as e:
            print(f"Rate limit exceeded. Waiting for {e.x} seconds.")
            await asyncio.sleep(e.x)
            return await get_group_info(chat_id)
        except Exception as e:
            print(f"Failed to get chat info for {chat_id}: {e}")

# def update_group_info():
#     url = getData(10)

async def get_group_msg(chat_id):
    api_id = '22019167'
    api_hash = '69e29ef1d74a7400f638bd481e8ea084'
    bot_token = '8096915404:AAHcnrQqmgNYEr8r0OVij5X7dGFcGOwxlaM'
    token_name = 'cjss_test01_bot'
    bot = Client(token_name, api_id=api_id, api_hash=api_hash, bot_token=bot_token)
    async with bot as client:
        # 获取历史消息
        flag = True
        while True:
            flag = False
            now = datetime.now()
            today_start = datetime(now.year, now.month, now.day)
            yesterday_start = today_start - timedelta(days=1)

            # 获取昨天的所有消息
            messages = []
            async for message in client.get_chat_history(chat_id, offset_date=today_start, limit=10):
                print(message)
            # async for message in client.get_chat_history(chat_id, offset_date=today_start):
            #     if yesterday_start <= message.date < today_start:
            #         messages.append(message)
            #     elif message.date < yesterday_start:
            #         break

            # 打印消息内容
            # for msg in messages:
            #     if not isinstance(msg.entities, list):
            #         continue
            #     for item in msg.entities:
            #         if isinstance(item, MessageEntityTextUrl):
            #             print(f"({item})")
            # messages = await client.get_chat_history(
            #     chat_id=g_username,
            #     limit=limit,
            #     offset_date=today_start,
            #     offset=0
            # )
            # msgs = messages.messages
            # if not msgs:
            #     break
            # if msgs[0].id > last_max_id:
            #     last_max_id = msgs[0].id
            # offset_id = msgs[-1].id
            # total_msg = total_msg + len(msgs)
            #
            # for msg in msgs:
            #     if not isinstance(msg.entities, list):
            #         continue
            #     for item in msg.entities:
            #         print(f"({item.url})")
            #         # if isinstance(item, MessageEntityTextUrl):
            #         #     msg_handler(f"({item.url})")
            #
            # if total_count_limit != 0 and total_msg >= total_count_limit:
            #     break
            # if len(msgs) < limit:
            #     # 到达频道的开始
            #     break

# async def get_msg_info():

async def get_me():
    api_id = '22019167'
    api_hash = '69e29ef1d74a7400f638bd481e8ea084'
    bot_token = '8096915404:AAHcnrQqmgNYEr8r0OVij5X7dGFcGOwxlaM'
    token_name = 'cjss_test01_bot'
    bot = Client(token_name, api_id=api_id, api_hash=api_hash, bot_token=bot_token)
    async with bot as client:
        print(await client.get_me())

def sendMsg(msg):
    API_ID = '26534384'
    API_HASH = '8c32b586b4c83b04b9ab7d50c6464907'
    BOT_TOKEN = '7931792484:AAEX1QM7483FqCpcRvwFBEdbr2_Hb6uWp5s'
    client = Client('myapp', API_ID, API_HASH, bot_token=BOT_TOKEN)
    with client:
        client.send_message("cjaniu", msg)

if __name__ == "__main__":
    asyncio.run(get_group_info(-1002248854833))

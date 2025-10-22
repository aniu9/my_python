import asyncio
import hashlib
import json
import re
import time
from datetime import timedelta, date

import requests
from pyrogram import idle, Client, handlers, errors, enums
from pytz import timezone

from tools.util import file_util

# 166 aniu923 阿牛
API_ID = '26534384'
API_HASH = '8c32b586b4c83b04b9ab7d50c6464907'

TOKEN_NAME = 'fq_collect_bot'
BOT_TOKEN = '7931792484:AAEX1QM7483FqCpcRvwFBEdbr2_Hb6uWp5s'

# 后端服务请求
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

def req_api(api, data):
    # 正式环境
    url = "http://a276b8d3ca3a14befa1dc6335eaa47ea-f83cb44aa303c283.elb.ap-southeast-1.amazonaws.com:8800/cms-service/"
    # 测试环境
    # url = "http://a901c69ff2d4c4b9bb678f3ebc6ea4c1-a52e4692b9f44640.elb.ap-southeast-1.amazonaws.com:8800/"

    url = url + api.strip("/")
    now = datetime.now()
    timestamp = int(now.timestamp() * 1000)
    j = json.dumps(data)
    server_app_key = "1"
    s = sign(str(server_app_key), timestamp, j)
    headers = {
        'Content-Type': 'application/json',
        "appkey": str(server_app_key),
        "timestamp": str(timestamp),
        "sign": s
    }

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

def get_to_data(size):
    url = "cms/content/getSupplyGroups"
    data = {"size": size}
    lst_data = req_api(url, data)
    urls = []
    for data in lst_data:
        urls.append(data["url"])
    return urls

# 机器人api
def get_bot(token_name=None, api_id=None, api_hash=None, bot_token=None):
    token_name = "my_bot" if token_name is None else token_name
    api_id = API_ID if api_id is None else api_id
    api_hash = API_HASH if api_hash is None else api_hash
    bot_token = BOT_TOKEN if bot_token is None else bot_token
    bot = Client(token_name, api_id=api_id, api_hash=api_hash, bot_token=bot_token)
    return bot

def get_app(token_name=None, api_id=None, api_hash=None):
    token_name = "my_app" if token_name is None else token_name
    api_id = API_ID if api_id is None else api_id
    api_hash = API_HASH if api_hash is None else api_hash
    client = Client(token_name, api_id, api_hash)
    return client

async def get_bot_info(token):
    async with get_bot(token, bot_token=token) as client:
        bot = await client.get_me()
        print(f"first_name: {bot.first_name}\nuname: {bot.username}\ntaken: {token}")

async def get_chat_info(chat_id):
    async with get_bot() as bot:
        print(f"{datetime.now()}")
        try:
            chat = await bot.get_chat(chat_id)
            print(f"Chat ID: {chat.id}")
            print(f"Chat Title: {chat.title}")
            print(f"Chat Username: {chat.username}")
            print(f"Chat Type: {chat.type}")
            print(f"User Num: {chat.members_count}")
            # print(chat)
        except errors.FloodWait as e:
            print(f"Rate limit exceeded. Waiting for {e.x} seconds.")
            await asyncio.sleep(e.x)
            return await get_chat_info(chat_id)
        except Exception as e:
            print(f"Failed to get chat info for {chat_id}: {e}")

async def get_chat_history(chat_id):
    # api_id = '22019167'
    # api_hash = '69e29ef1d74a7400f638bd481e8ea084'
    # bot_token = '8096915404:AAHcnrQqmgNYEr8r0OVij5X7dGFcGOwxlaM'
    # token_name = 'cjss_test01_bot'
    API_ID = 21975583
    API_HASH = '038ebc70dadb32be863b086e7b1ed93b'
    BOT_TOKEN = '7442834041:AAGPY8ffSLPajqLx1KN1GiWPB4yWtj9Y9U0'
    bot = Client("bot1", API_ID, API_HASH, bot_token=BOT_TOKEN)
    await bot.start()
    async with get_app() as client:
        # await client.send_message(-1002465447452,"abc")
        # old_msg = await client.get_messages(-1002406457086, 6)
        # print(old_msg)
        # return old_msg

        # 获取历史消息
        flag = True
        i = 1
        while i == 1:
            i = i + 1
            flag = False
            now = datetime.now()
            today_start = datetime(now.year, now.month, now.day)
            yesterday_start = today_start - timedelta(days=1)

            # 获取昨天的所有消息
            messages = []
            async for message in client.get_chat_history(chat_id, offset_id=0, limit=2, reverse=False):
                print(message.id, message.date)
                # await bot.copy_message(chat_id, chat_id, message_id=message.id)
                # await bot.copy_media_group(chat_id, chat_id, message_id=message.id)
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

    await bot.stop()

# async def get_msg_info():


def send_msg(msg):
    # api_id = '26534384'
    # api_hash = '8c32b586b4c83b04b9ab7d50c6464907'
    # bot_token = '7931792484:AAEX1QM7483FqCpcRvwFBEdbr2_Hb6uWp5s'
    with get_bot() as bot:
        bot.send_message("cjaniu", msg)

def req_api1(api, data):
    # 正式环境
    # url = "http://a276b8d3ca3a14befa1dc6335eaa47ea-f83cb44aa303c283.elb.ap-southeast-1.amazonaws.com:8800/cms-service/"
    # 测试环境
    # url = "http://a901c69ff2d4c4b9bb678f3ebc6ea4c1-a52e4692b9f44640.elb.ap-southeast-1.amazonaws.com:8800/"
    # 开发环境
    url = "http://localhost:8090/"
    url = url + api.strip("/")
    # data = json.dumps(data)
    headers = {
        'Content-Type': 'application/json'
    }

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
    except Exception as ex:
        print(ex)

from datetime import datetime
from pyrogram.types import Message, CallbackQuery, User, Chat, MessageEntity, Photo, Document, InlineKeyboardMarkup, \
    InlineKeyboardButton

def map_user(user: User) -> dict:
    """将 Pyrogram User 转换为 Telegram Bot API 格式"""
    return {
        "id": user.id,
        "is_bot": user.is_bot,
        "first_name": user.first_name,
        "last_name": user.last_name or None,
        "username": user.username or None,
        "language_code": user.language_code or None
    } if user else None

def map_chat(chat: Chat) -> dict:
    """将 Pyrogram Chat 转换为 Telegram Bot API 格式"""
    return {
        "id": chat.id,
        "type": chat.type.value,
        "title": chat.title or None,
        "username": chat.username or None,
        "first_name": chat.first_name or None,
        "last_name": chat.last_name or None
    } if chat else None

def map_message_entity(entity: MessageEntity) -> dict:
    """将 MessageEntity 转换为 Telegram Bot API 格式"""
    return {
        "type": entity.type.value,
        "offset": entity.offset,
        "length": entity.length,
        "url": entity.url or None,
        "user": map_user(entity.user) if entity.user else None,
        "language": entity.language or None,
        "custom_emoji_id": entity.custom_emoji_id or None,
    } if entity else None

def map_photo(photo: Photo) -> dict:
    """将 Photo 转换为文件字典（仅包含文件ID）"""
    return {"file_id": photo.file_id} if photo else None

def map_document(doc: Document) -> dict:
    """将 Document 转换为 Telegram Bot API 格式"""
    return {
        "file_id": doc.file_id,
        "file_name": doc.file_name or None,
        "mime_type": doc.mime_type or None,
        "file_size": doc.file_size or None,
    } if doc else None

def map_message(message: Message) -> dict:
    """将 Pyrogram Message 转换为 Telegram Bot API 格式"""
    if not message:
        return None

    # 基础字段
    msg_dict = {
        "message_id": message.id,
        "from": map_user(message.from_user),
        "date": int(message.date.timestamp()) if message.date else None,
        "chat": map_chat(message.chat),
        "text": message.text or None,
        "caption": message.caption or None,
        # "entities": [map_message_entity(e) for e in message.entities] if message.entities else None,
        "caption_entities": [map_message_entity(e) for e in
                             message.caption_entities] if message.caption_entities else None,
    }

    # 处理媒体内容
    if message.photo:
        msg_dict["photo"] = [map_photo(p) for p in message.photo]
    elif message.document:
        msg_dict["document"] = map_document(message.document)
    elif message.audio:
        msg_dict["audio"] = {
            "file_id": message.audio.file_id,
            "duration": message.audio.duration,
            "title": message.audio.title or None,
            "mime_type": message.audio.mime_type or None,
        }
    elif message.video:
        msg_dict["video"] = {
            "file_id": message.video.file_id,
            "width": message.video.width,
            "height": message.video.height,
            "duration": message.video.duration,
        }
    # 其他媒体类型（如 location, poll 等）按需扩展...

    # 处理转发和回复消息
    if message.forward_from:
        msg_dict["forward_from"] = map_user(message.forward_from)
    if message.forward_from_chat:
        msg_dict["forward_from_chat"] = map_chat(message.forward_from_chat)
    if message.reply_to_message:
        msg_dict["reply_to_message"] = map_message(message.reply_to_message)

    return msg_dict

def map_callback_query(callback: CallbackQuery) -> dict:
    """将 CallbackQuery 转换为 Telegram Bot API 格式"""
    return {
        "id": callback.id,
        "from": map_user(callback.from_user),
        "message": map_message(callback.message) if callback.message else None,
        "data": callback.data,
        "inline_message_id": callback.inline_message_id or None,
    } if callback else None

def convert_to_telegram_update(message: Message = None, callback: CallbackQuery = None) -> dict:
    """生成与 Telegram Bot API Update 兼容的 JSON"""
    update = {}
    if message:
        update["message"] = map_message(message)
    elif callback:
        update["callback_query"] = map_callback_query(callback)
    return update

bot = Client(TOKEN_NAME, api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

@bot.on_message()
def start_command(client, message: Message):
    print('client')
    req_api1("/gcmgt/process", {"botId": 7931792484, "update": convert_to_telegram_update(message)})

@bot.on_callback_query()
async def handle_home_action(client: Client, msg: CallbackQuery):
    print('command')
    req_api1("/gcmgt/process", {"botId": 7931792484, "update": convert_to_telegram_update(None, msg)})

# @Client.on_message()
async def handle_home_action(client: Client, msg: Message):
    print('text')
    req_api("/gcmgt/test", {"botId": 7931792484, "update": msg})

# @Client.on_callback_query()
async def handle_slash_submit(client: Client, msg: Message):
    print('button')
    req_api("/gcmgt/test", {"botId": 7931792484, "update": msg})

customHandlers = [
    handlers.MessageHandler(
        callback=handle_home_action
    ),
]

for cHandler in customHandlers:
    bot.add_handler(cHandler)

async def main():
    async with bot:
        await idle()
        await bot.stop()

async def reply():
    async with get_bot() as app:
        await app.send_message('testan03', "test", reply_to_message_id=29, reply_to_chat_id="rtgokn")

async def get_msg():
    async with get_bot() as app:
        old_msg = await app.get_messages("testcl01", 229)
        print(old_msg)

async def delete_msg():
    app = Client(TOKEN_NAME, api_id=API_ID, api_hash=API_HASH,
                 bot_token='6854483464:AAEAIZoirC7URAVajssHby4ZHzox9EduPIM')
    await app.start()
    # await app.send_message(-1002189485368, "\u200B\ntest")
    # await app.get_chat(-1002189485368)
    await app.delete_messages(-1002232206477, [5317, 5318, 5319, 5320])
    await app.stop()

async def replay_msg():
    app = Client(TOKEN_NAME, api_id=API_ID, api_hash=API_HASH,
                 bot_token=BOT_TOKEN)
    await app.start()
    # await app.send_message(-1002189485368, "\u200B\ntest")
    # await app.get_chat(-1002189485368)
    await app.send_message("testcl01", "abc", reply_to_message_id=11)
    await app.stop()

async def edit_message_reply_markup():
    async with get_bot() as bot:
        await bot.edit_message_reply_markup(
            chat_id='testpost88',
            message_id=63,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(text="test", url="https://t.me/seek")]])
        )

async def send_message():
    async with get_bot() as bot:
        await bot.send_message(chat_id='testcl01', text='test')

async def collect_message(chat_ids):
    chat_ids = chat_ids.split(',')
    for chat_id in chat_ids:
        async with get_app() as client:
            count = 0
            data = []
            date = datetime.today().now().date() - timedelta(days=1)
            async for message in client.get_chat_history(chat_id, reverse=False):
                print(chat_id, message.id, message.date)
                if message.date.date() < date:
                    break
                count = count + 1
                tz = timezone('Asia/Shanghai')
                local_time = message.date.astimezone(tz)
                msgdate = local_time.strftime("%Y-%m-%d %H:%M:%S")
                text = message.text or message.caption
                if not text:
                    continue
                if message.chat.username == 'hwcg':
                    text = text.split('\n')[0]
                    encoded = text.encode('utf-8')[:45]
                    # 忽略不完整的UTF-8字符
                    text = encoded.decode('utf-8', 'ignore')
                data.append({
                    "url": f"https://t.me/{message.chat.username}",
                    "linkType": 1,
                    "status": 1,
                    "telegramId": message.chat.id,
                    "source": 0,
                    "msg": {
                        "url": f"https://t.me/{message.chat.username}/{message.id}",
                        "text": text,
                        "linkType": getLinkType(message),
                        "publishTime": message.date.strftime('%Y-%m-%d %H:%M:%S')
                    }
                })
                if count % 20 == 0:
                    await post_cms_message(data)
                    data = []
                    await asyncio.sleep(5)
            if len(data) > 0:
                await post_cms_message(data)

def getLinkType(msg: Message):
    # 2- Vedio视频消息 3- Photo图片消息 4- Text文本消息 5- Audio音乐消息 6- File文件消息
    linkType = 4
    if msg.video:
        linkType = 2
    if msg.photo:
        linkType = 3
    if msg.audio:
        linkType = 5
    if msg.document:
        linkType = 6
    return linkType

async def post_cms_message(data):
    # 正式环境
    url = "http://a276b8d3ca3a14befa1dc6335eaa47ea-f83cb44aa303c283.elb.ap-southeast-1.amazonaws.com:8800/cms-service"
    # 测试环境
    # url = "http://a901c69ff2d4c4b9bb678f3ebc6ea4c1-a52e4692b9f44640.elb.ap-southeast-1.amazonaws.com:8800/"

    url = url + '/cms/content/collect'
    now = datetime.now()
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

async def main2():
    urls = file_util.read_first_csv(file_util.get_run_path_file("group_url.csv"))
    print(f"Len {len(urls)} to verify....")
    cmsPayloads = []
    bot_tokens = ['8233763050:AAEpI5wmk_iGgVRqxHm0PKIlj60k6BfdfDQ',
                  '7480786871:AAGocBV8ne1jli8HXzlC931rIj1CunpUhgg',
                  '7578200869:AAGYOb4_k3LiYDEymY8FfK_X2spdnOw0DAI',
                  '8096915404:AAHcnrQqmgNYEr8r0OVij5X7dGFcGOwxlaM',
                  '8081129756:AAHtoZ3s4lrP5u9Zf5tp2UNpVkGSXFZokqQ',
                  '7815275035:AAG3EPj39IommOSpS6AW_uNEcYH3uRGwJcs',
                  '7636152888:AAHj1w2GgOhSAA6WXeSjtdZFwj9e054s6xM',
                  '7761635217:AAGUWbkE2DhitbmHlO8yrmy8iN821v7epQc',
                  '7908663209:AAGC4CVFr5NRio9mOP8tQfqqaorirhPzzT0',
                  '7781291686:AAH3L0XGwzkhtJvyeJzhC-Dy2D71NVu-a3k',
                  '7491446987:AAE7EJbnNKZJ8vPWAUSeE3VbUoaS2EGCShc',
                  '8083490646:AAHR-gDwND7RHdsdrgsaeYbr6u5rSwdk-Fo',
                  '7623740381:AAEvJe8J3aaQQDcs7lZ-mGjR6dPKpmbrY0A',
                  '7628212809:AAHchf6SynOtcMxTu1H3x76AJ75qOfGsJM8',
                  '7514567164:AAFs9eEi8l7SwLtao7FnKtVyp4jnM5YBP7E',
                  '7639140436:AAFtftlffnWMT2PhQYbUbowf3MWCGJzJlEw',
                  '7421747425:AAGPnduuFE_ObGioI8zig9zzzb_Zq_9fh98',
                  '7523917878:AAEiwdcp4ErBYvUAJGkfkBcLzBGuJxW0c5k',
                  '7683228049:AAHEAcX4HP7LS-4-XkSsg2Ayz3ux5aFw7Bo',
                  '7730100615:AAENhqpbUZ71dxvjqiT1mwV4aYEe2_lWIWQ',
                  '7656464463:AAGuLTdATw82p5Sgi9ReNLsHcKJd_CjxIlA',
                  '7684315801:AAEZaA4JSc0vqzyj7oYlZPypAQKntDnvZ-U',
                  '8058289985:AAGHWd7NEuTFOnUlm3j87qV4vuSQy701uYM',
                  '7982066389:AAEP4utuXiwqyX9ckRbxfrhVKHTBPO3QPPs',
                  '8008813160:AAH7sRk1CLIYrVpyD5a_Rvt0c56F6_JToJc',
                  '7955910599:AAGd516_70m6s3btQDmvGm3j5epT5bNFYN0',
                  '7522630536:AAF_Rg5FDc-v-Yue8jXWKFKGDrUDJyJ1GQY',
                  '7660194863:AAF3Q758I9nYOqEDQ8xPVFlDajiTD8Mrhvc',
                  '8071671895:AAEtQNy5eWxlNhDB4LErNXpUKQAMlszc6cY',
                  '7749326865:AAGxHrNdMWesBZjRJmsBLVvYlWwXYBk-BF4',
                  '7768370445:AAGb4bPl6ZbNgCVn1YLS9dvEhH6d91gefzA',
                  '8015829310:AAEVRYcLZQA_rGrXMRPx-jgqZoDMkgchXng',
                  '7563281423:AAFFnpsJQRTh611sHw9rvH1sYr6uSNhicpE',
                  '7790147577:AAH2kSx4cu90vAKOe6BaeZa5LGyMg9sROUE',
                  '7374540154:AAENqpVJ8Je-96oWy3yYtzDoP7YZZK2hUqE',
                  '7758866458:AAFiqUfNLcX4f0lnGYf3TA5y5a7n0kCDLFo',
                  '7855095109:AAGm6SYGYD6uIFXrgxBz4MzjDonHojMztQw',
                  '7926115352:AAF02eTX0NvtLe9d_BmzYQQi9-9G-fc8ywc',
                  '7928713177:AAHhVU1URWDrJcCbc-gFPOcANkej0NvYtVg',
                  '7730729867:AAFC3ew6FnBgKrr25aHT3F4F16EBhkqGsYA',
                  '7999593657:AAG700aXejuQ6W9-9U70J3KUh_AfXDOIHjU',
                  '7689969798:AAF82-2xWaP3M-p1biOKcYrO5A2w9truz0Q',
                  '7608158540:AAG4mHYnuBIoJC3p-nB4Khv1_yQZQliFWVc',
                  '8117863460:AAGrQt0yc-FmQEcrmjeqwZTY8v3A3Jtm-og']
    bot_index = 0
    client = None
    ind = 0
    while ind < len(urls):
        url = urls[ind]
        ind = ind + 1
        pattern = r'https:\/\/t.me\/(.*)'
        matching = re.match(pattern=pattern, string=url)
        if not matching: continue
        username = matching.groups()[0]
        try:
            if client is None or ind % 20 == 0:
                if client:
                    await client.stop()
                bot_token = bot_tokens[bot_index]
                bot_index = bot_index + 1;
                if bot_index == 43:
                    bot_index = 0
                client = Client(bot_token, api_id=API_ID, api_hash=API_HASH, bot_token=bot_token)
                await client.start()
            cmsPayloads = []
            print(f"Fetching: {username} of {ind}/{len(urls)}, bot: {bot_index}")
            chat = await client.get_chat(username, force_full=True)
            if chat.type == enums.ChatType.CHANNEL:
                chat_type = 1
            elif chat.type == enums.ChatType.GROUP or chat.type == enums.ChatType.SUPERGROUP:
                chat_type = 0
            else:
                raise ValueError(f"Not a group or channel url: {url}")
            cmsPayloads.append({
                "isValid": 1,  # 0 无效 1有效
                "url": url,
                "tgId": chat.id,
                "linkType": chat_type,
                "userNum": chat.members_count,
                "userName": chat.username or '',
                "title": chat.title,
                "description": chat.description or '',
                "isIOS": 1 if chat.is_restricted else 0
            })
        except (errors.PeerIdInvalid, errors.UsernameNotOccupied, ValueError) as e:
            cmsPayloads.append({
                "isValid": 0,  # 0 无效 1有效
                "url": url,
            })
        except Exception as e:
            if 'FLOOD_WAIT' in str(e):
                ind = ind - 1
                print('FLOOD_WAIT:' + str(e))
                if client:
                    await client.stop()
                bot_token = bot_tokens[bot_index]
                bot_index = bot_index + 1;
                if bot_index == 43:
                    bot_index = 0
                client = Client(bot_token, api_id=API_ID, api_hash=API_HASH, bot_token=bot_token)
                await client.start()
            else:
                print(str(e))
        if len(cmsPayloads) > 0:
            url = "http://a276b8d3ca3a14befa1dc6335eaa47ea-f83cb44aa303c283.elb.ap-southeast-1.amazonaws.com:8800/cms-service"
            url = url + '/cms/content/collectGroupInfo'
            headers = {"Content-Type": "application/json", "gateway_internal_request_flag": "dhasjkdhaskgvhcbxahsa"}

            try:
                response = requests.post(url, headers=headers, json=cmsPayloads, timeout=30)
                if response.status_code != 200:
                    print({"error": f"Status code: {response.status_code}"})
                result = json.loads(response.content)
                if result["code"] != 200:
                    print(f"error: {result}")
            except Exception as e:
                print(str(e))
        time.sleep(1)

if __name__ == "__main__":
    # 7869992066:AAE0j_-uSpKlPWVES3a3mqr-yg04-LLAJhE
    # ,7642211861:AAEkh_wmw1TZ080ymTVuzab0Mifr6oJJ3Mw
    # ,8150574555:AAEPtjWX-BAPFt69NzJ_ZAlN0bvolRi8_mU
    # ,8018823188:AAHuLZzZO8nT_0AjE7yOqqD_cTDl_r_wr9M
    # asyncio.run(send_message())
    # asyncio.run(get_chat_info('seeknrzzz'))
    # asyncio.run(get_chat_history('hwcg'))
    # asyncio.run(collect_message('hwcg'))
    # asyncio.run(collect_message('hwcg,hwnr8,hwsj'))
    # asyncio.run(collect_message('rbshangpian,spjiaoliu,gcjxvdo,jhsavscj,dyywvdo,mdgcthjq,onlyfs_sm,chiguahiliao,rqfanchab,tyhjluanlun'))
    # bot.run(main())
    # asyncio.run(get_bot_info("8174398578:AAGaPOnPNUP6Cuq30dN7J0SpVP7EKXADWZ4"))
    # asyncio.run(get_msg())
    # asyncio.run(delete_msg())
    # asyncio.run(replay_msg())
    # asyncio.run(get_bot_info('8274123427:AAEqlcZj0gPombB_080IpA7OjJhz0f2lGmA'))
    # asyncio.run(edit_message_reply_markup())
    asyncio.run(main2())

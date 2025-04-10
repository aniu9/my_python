import asyncio
import hashlib
import json
from datetime import datetime, timedelta

import requests
from pyrogram import errors, idle, handlers
from pyrogram import filters, Client
from pyrogram.types import Message

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

async def get_me_info(client):
    async with client as cl:
        chat = await cl.get_me()
        print(chat)

async def get_chat_info(chat_id):
    async with get_bot() as bot:
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
            return await get_chat_info(chat_id)
        except Exception as e:
            print(f"Failed to get chat info for {chat_id}: {e}")

async def get_chat_history(chat_id):
    # api_id = '22019167'
    # api_hash = '69e29ef1d74a7400f638bd481e8ea084'
    # bot_token = '8096915404:AAHcnrQqmgNYEr8r0OVij5X7dGFcGOwxlaM'
    # token_name = 'cjss_test01_bot'
    async with get_app() as client:
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
from pyrogram.types import Message, CallbackQuery, User, Chat, MessageEntity, Photo, Document, Audio, Video, Animation, Sticker, Contact, Location, Venue, Poll, Dice, Game
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
        "caption_entities": [map_message_entity(e) for e in message.caption_entities] if message.caption_entities else None,
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
def start_command(client, message:Message):
    print('client')
    req_api1("/gcmgt/process",{"botId":7931792484,"update":convert_to_telegram_update(message)})

@bot.on_callback_query()
async def handle_home_action(client: Client, msg: CallbackQuery):
    print('command')
    req_api1("/gcmgt/process",{"botId":7931792484,"update":convert_to_telegram_update(None,msg)})

# @Client.on_message()
async def handle_home_action(client: Client, msg: Message):
    print('text')
    req_api("/gcmgt/test",{"botId":7931792484,"update":msg})

# @Client.on_callback_query()
async def handle_slash_submit(client: Client, msg: Message):
    print('button')
    req_api("/gcmgt/test",{"botId":7931792484,"update":msg})

async def main():
    async with bot:
        await idle()
        await bot.stop()

if __name__ == "__main__":
    # asyncio.run(get_chat_info("fq_collect_bot"))
    # asyncio.run(get_chat_history('testan03'))
    bot.run(main())

customHandlers = [
    handlers.MessageHandler(
        callback= handle_home_action
    ),
]

for cHandler in customHandlers:
    bot.add_handler(cHandler)

import asyncio
import os
import sqlite3
from datetime import datetime

# from telegram import Bot
# from pyrogram import Client, enums
from telethon import TelegramClient, events
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.functions.users import GetFullUserRequest

# 166 cobb923 fq_collect_bot
api_id = '26534384'
api_hash = '8c32b586b4c83b04b9ab7d50c6464907'
bot_token = '7931792484:AAEX1QM7483FqCpcRvwFBEdbr2_Hb6uWp5s'

# 中文搜索机器人 '7047022058:AAFG91BlbMmLlC4x4UevEe_TEax4jNdsaYU'
# chaojiAdmin_bot '7312534721:AAEA-EzWGnpwHsMmWI4pk6R2oxU8Rf1xUrw'
# CJSS_AlarmBot 6854483464:AAEAIZoirC7URAVajssHby4ZHzox9EduPIM

# chaojianiu 超级阿牛
# cjss_my_bot 7480786871:AAGocBV8ne1jli8HXzlC931rIj1CunpUhgg
# cjss_cj_bot 7578200869:AAGYOb4_k3LiYDEymY8FfK_X2spdnOw0DAI
# cjss_test01_bot 8096915404:AAHcnrQqmgNYEr8r0OVij5X7dGFcGOwxlaM
# cjss_cj001_bot 8081129756:AAHtoZ3s4lrP5u9Zf5tp2UNpVkGSXFZokqQ
# cjss_cj002_bot 7815275035:AAG3EPj39IommOSpS6AW_uNEcYH3uRGwJcs
# cjss_cj003_bot 7636152888:AAHj1w2GgOhSAA6WXeSjtdZFwj9e054s6xM
# cjss_cj004_bot 7761635217:AAGUWbkE2DhitbmHlO8yrmy8iN821v7epQc
# cjan_001_bot 7908663209:AAGC4CVFr5NRio9mOP8tQfqqaorirhPzzT0
# cjan_002_bot 7781291686:AAH3L0XGwzkhtJvyeJzhC-Dy2D71NVu-a3k
# cjan_003_bot 7491446987:AAE7EJbnNKZJ8vPWAUSeE3VbUoaS2EGCShc
# cjan_004_bot 8083490646:AAHR-gDwND7RHdsdrgsaeYbr6u5rSwdk-Fo
# cjan_005_bot 7623740381:AAEvJe8J3aaQQDcs7lZ-mGjR6dPKpmbrY0A
# cjan_006_bot 7628212809:AAHchf6SynOtcMxTu1H3x76AJ75qOfGsJM8
# cjan_007_bot 7514567164:AAFs9eEi8l7SwLtao7FnKtVyp4jnM5YBP7E
# cjan_008_bot 7639140436:AAFtftlffnWMT2PhQYbUbowf3MWCGJzJlEw
# cjan_009_bot 7421747425:AAGPnduuFE_ObGioI8zig9zzzb_Zq_9fh98
# cjan_010_bot 7523917878:AAEiwdcp4ErBYvUAJGkfkBcLzBGuJxW0c5k
# cjan_011_bot

# 166 cobb923 阿牛 26534384 8c32b586b4c83b04b9ab7d50c6464907
# fq_collect_bot 7931792484:AAEX1QM7483FqCpcRvwFBEdbr2_Hb6uWp5s
# fq_cj001_bot 7543527702:AAFsPCyT9Wv4_YxuHT0i8IzNz2s1i4Mr6JU
# fq_cj002_bot 7699660452:AAFIm4NKhsWtpxYAyDJnWDA6ylYwHifXW4k
# fq_cj003_bot 7683228049:AAHEAcX4HP7LS-4-XkSsg2Ayz3ux5aFw7Bo
# an_001_bot 7730100615:AAENhqpbUZ71dxvjqiT1mwV4aYEe2_lWIWQ
# an_002_bot 7656464463:AAGuLTdATw82p5Sgi9ReNLsHcKJd_CjxIlA
# an_003_bot 7684315801:AAEZaA4JSc0vqzyj7oYlZPypAQKntDnvZ-U
# an_004_bot 8058289985:AAGHWd7NEuTFOnUlm3j87qV4vuSQy701uYM
# an_005_bot 7982066389:AAEP4utuXiwqyX9ckRbxfrhVKHTBPO3QPPs
# an_006_bot 8008813160:AAH7sRk1CLIYrVpyD5a_Rvt0c56F6_JToJc
# an_007_bot 7955910599:AAGd516_70m6s3btQDmvGm3j5epT5bNFYN0
# an_008_bot 7522630536:AAF_Rg5FDc-v-Yue8jXWKFKGDrUDJyJ1GQY
# an_009_bot 7660194863:AAF3Q758I9nYOqEDQ8xPVFlDajiTD8Mrhvc
# an_011_bot 8071671895:AAEtQNy5eWxlNhDB4LErNXpUKQAMlszc6cY
# an_012_bot 7749326865:AAGxHrNdMWesBZjRJmsBLVvYlWwXYBk-BF4
# an_013_bot 7768370445:AAGb4bPl6ZbNgCVn1YLS9dvEhH6d91gefzA
# an_014_bot 8015829310:AAEVRYcLZQA_rGrXMRPx-jgqZoDMkgchXng
# an_015_bot 7563281423:AAFFnpsJQRTh611sHw9rvH1sYr6uSNhicpE

# 158 wx 22019167 69e29ef1d74a7400f638bd481e8ea084 fqwapp
# fq_w001_bot 7790147577:AAH2kSx4cu90vAKOe6BaeZa5LGyMg9sROUE
# fq_w002_bot 7374540154:AAENqpVJ8Je-96oWy3yYtzDoP7YZZK2hUqE
# fq_w003_bot 7758866458:AAFiqUfNLcX4f0lnGYf3TA5y5a7n0kCDLFo
# fq_w004_bot 7855095109:AAGm6SYGYD6uIFXrgxBz4MzjDonHojMztQw
# fq_w005_bot 7926115352:AAF02eTX0NvtLe9d_BmzYQQi9-9G-fc8ywc
# fq_w006_bot 7928713177:AAHhVU1URWDrJcCbc-gFPOcANkej0NvYtVg
# fq_w007_bot 7730729867:AAFC3ew6FnBgKrr25aHT3F4F16EBhkqGsYA
# fq_w008_bot 7999593657:AAG700aXejuQ6W9-9U70J3KUh_AfXDOIHjU
# fq_w009_bot 7689969798:AAF82-2xWaP3M-p1biOKcYrO5A2w9truz0Q
# fq_w010_bot 7608158540:AAG4mHYnuBIoJC3p-nB4Khv1_yQZQliFWVc
# fq_w011_bot 8117863460:AAGrQt0yc-FmQEcrmjeqwZTY8v3A3Jtm-og

# chaojianiu
user_id = '5380352615'

# 测试群
# group_id = 'mygroup423'
# # 小说群
# group_id = 'TalesforHer'
# 招聘群
group_id = 'chaojizpq'


async def getBot():
    client = getClient()
    await client.start(bot_token=bot_token)
    return client


def getClient():
    client = TelegramClient('myapp', api_id, api_hash)
    return client


# def getBot():
#     bot = Bot(token=bot_token)
#     return bot
# def getPyClient():
#     client = Client('my_app', api_id, api_hash)
#     return client
#
#
# def getPyBot():
#     bot = Client('my_bot', api_id=api_id, api_hash=api_hash, bot_token=bot_token)
#     return bot


async def sendMsg():
    bot = getBot()
    await bot.send_message(user_id, "<b>简</b><em>单</em><em>查</em><em>询</em>字符串", "HTML")


async def getBotId():
    # https://api.telegram.org/bot7480786871:AAGocBV8ne1jli8HXzlC931rIj1CunpUhgg/getMe
    bot = getBot()
    me = await bot.get_me()
    print(me.name + ":" + str(me.id))


async def getGroupId():
    bot = getClient()
    updates = await bot.getUpdates()
    print(updates[0].effective_chat.id)


async def get_chat_members():
    bot = getBot()

    members = await bot.banChatMember('chaojizpq')
    for member in members:
        print(f"用户名：{member.username}，姓名：{member.first_name} {member.last_name}，ID：{member.id}")

    # async def get_chat_member():
    #     # aniu
    #     members = await tg.get_chat_administrators(-4598624895)
    #     # zp
    #     # members = await tg.get_chat_member_count(2033752932)
    #     print(members)
    #
    # asyncio.run(get_chat_member())


# def get_group_members():
#     client = getPyClient()
#
#     async def get_group_members(group_username):
#         client.start()
#         group = client.get_chat_members(group_username)
#         async for member in client.get_chat_members(group_username):
#             print(member)
#
#         return group
#
#     # 填写你要采集成员的群组用户名
#     group_username = 'mygroup423'
#
#     # 采集群组成员
#     members = client.loop.run_until_complete(get_group_members(group_username))
#
#     # 打印成员信息
#     data = []
#     for member in members:
#         data.append([member.id, member.username, member.first_name])
#         print(f"用户名：{member.username}，姓名：{member.first_name} {member.last_name}，ID：{member.id}")
#
#     # 关闭Telegram客户端
#     client.disconnect()
#
#     cur_dir = os.path.dirname(__file__)
#     file_tg_url = os.path.join(cur_dir, 'users.csv')
#     with open(file_tg_url, "w", newline="") as csvfile:
#         writer = csv.writer(csvfile)
#         writer.writerows(data)
#

def get_group_members1():
    async def get_users(client, group_id):
        async for user in client.get_peer_id(group_id):
            if not user.deleted:
                print("id:", user.id, "username:", user.username)

    bot = TelegramClient('tg', '26534384', '8c32b586b4c83b04b9ab7d50c6464907').start(
        bot_token='7162204476:AAF25-D9PY81QdL7802hxgtVFtg_LYe1joo')

    with bot:
        asyncio.get_event_loop().run_until_complete(get_users(bot, 'chaojizpq'))

    app = bot

    async def main():
        async with app:
            async for member in app.get_chat_members(group_id, limit=10):
                print(f"用户名：{member.user.username}，姓名：{member.user.first_name}，ID：{member.user.id}")
                data.append([member.id, member.username, member.first_name])

    app.run(main())


def save_to_sqlite(datas):
    root_dir = os.path.dirname(os.path.dirname(__file__))
    conn = sqlite3.connect(root_dir + '/telegram_data.db')
    c = conn.cursor()

    # 创建表（如果不存在）
    c.execute('''
        create table IF NOT EXISTS tg_user
        (
            userId    bigint,
            userName  varchar(100),
            firstName varchar(100),
            createTime datetime,
            updateTime datetime
        );
    ''')

    now = datetime.now()
    for content in datas:
        userId = content[0]
        userName = content[1]
        firstName = content[2]
        if userId:
            # 检查 userId 是否已存在
            c.execute('SELECT COUNT(1) FROM tg_user WHERE userId = ?', (userId,))
            exists = c.fetchone()[0] > 0

            if exists:
                # 更新现有记录的 updated_at
                c.execute('''
                    UPDATE tg_user
                    SET userName = ?,
                        firstName = ?,
                        updateTime = ?
                    WHERE userId = ?
                ''', (
                    userId,
                    userName,
                    firstName,
                    now
                ))
            else:
                # 插入新记录
                c.execute('''
                    INSERT INTO tg_user (
                        userId, 
                        userName, 
                        firstName, 
                        createTime
                    ) VALUES (?, ?, ?, ?)
                ''', (
                    userId,
                    userName,
                    firstName,
                    now
                ))

    conn.commit()
    conn.close()
    return


# def get_group_members2():
#     app = getPyBot()
#     data = []
#
#     async def main():
#         async with app:
#             async for member in app.get_chat_members(group_id, limit=2000):
#                 # print(f"用户名：{member.user.username}，姓名：{member.user.first_name}，ID：{member.user.id}")
#                 data.append([member.user.id, member.user.username, member.user.first_name])
#
#     app.run(main())
#
#     save_to_sqlite(data)
#     # cur_dir = os.path.dirname(__file__)
#     # file_tg_url = os.path.join(cur_dir, 'tg_user.csv')
#     # with open(file_tg_url, "w", newline="") as csvfile:
#     #     writer = csv.writer(csvfile)
#     #     writer.writerow(["userId","userName","firstName"])
#     #     writer.writerows(data)


def get_group_msgs(group_username):
    client = getClient()

    # group_username = 'fqcollect'

    async def get_msgs(group_username):
        client.start(bot_token=bot_token)
        # 获取群组实体
        entity = await client.get_entity(group_username)
        # 获取历史消息
        messages = await client(GetHistoryRequest(
            peer=entity,
            limit=10,  # 获取的消息数量
            offset_date="2024-11-02",
            offset_id=0,
            max_id=0,
            min_id=0,
            add_offset=0,
            hash=0
        ))
        return messages

        # 打印消息
        for message in messages.messages:
            print(message.sender_id, ':', message.text)

    # 采集群消息
    messages = client.loop.run_until_complete(get_msgs(group_username))
    data = []
    for msg in messages:
        print(f"{msg.text}")

    # 关闭Telegram客户端
    client.disconnect()

    # cur_dir = os.path.dirname(__file__)
    # file_tg_url = os.path.join(cur_dir, 'users.csv')
    # with open(file_tg_url, "w", newline="") as csvfile:
    #     writer = csv.writer(csvfile)
    #     writer.writerows(data)


def forward(client):
    # 定义转发规则
    forward_rules = {
        'source_chat_id': 'target_chat_id',  # 从源聊天转发到目标聊天
        # 可以添加更多规则
    }

    @client.on(events.NewMessage(chats=list(forward_rules.keys())))
    async def forward_handler(event):
        # 获取消息的协议号
        protocol_number = event.message.id

        # 获取源聊天 ID
        source_chat_id = event.chat_id

        # 获取目标聊天 ID
        target_chat_id = forward_rules[str(source_chat_id)]

        # 构造转发的消息
        forwarded_message = f"协议号 {protocol_number}:\n\n{event.message.text}"

        # 转发消息到目标聊天
        await client.send_message(target_chat_id, forwarded_message)

        print(f"消息已从 {source_chat_id} 转发到 {target_chat_id}")


async def get_user_info(user_id):
    try:
        client = getClient()
        # 获取用户实体
        user = await client.get_entity(user_id)

        # 获取完整的用户信息
        full_user = await client(GetFullUserRequest(user))

        # 提取并返回用户信息
        print({
            'id': user.id,
            'first_name': user.first_name,
            'last_name': user.last_name,
            'username': user.username,
            'phone': user.phone,
            'bio': full_user.about,
            'is_bot': user.bot,
            'mutual_contact': user.mutual_contact,
            'restricted': user.restricted,
            'verified': user.verified,
            'photo': user.photo,
            'status': user.status,
            'last_online': user.status.was_online if user.status else None,
        })
    except Exception as e:
        print(f"获取用户信息时出错: {e}")
        return None

import asyncio
import aiohttp
import logging
from telegram import Bot
from telegram.error import TelegramError
from telegram.constants import ChatMemberStatus
import aiosqlite
import time
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TelegramMassGroupInfoFetcher:
    def __init__(self, bot_token, db_path='group_info.db', cache_duration=3600, rate_limit_per_minute=20):
        self.bot = Bot(token=bot_token)
        self.db_path = db_path
        self.cache_duration = cache_duration
        self.rate_limit = asyncio.Semaphore(rate_limit_per_minute)
        self.last_reset = time.time()
        self.rate_limit_per_minute = rate_limit_per_minute

    async def init_db(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''
                CREATE TABLE IF NOT EXISTS group_info (
                    chat_id INTEGER PRIMARY KEY,
                    title TEXT,
                    type TEXT,
                    members_count INTEGER,
                    admins TEXT,
                    description TEXT,
                    last_updated TIMESTAMP
                )
            ''')
            await db.commit()

    async def get_chat_info(self, chat_id):
        current_time = datetime.now()

        # 检查数据库缓存
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute('SELECT * FROM group_info WHERE chat_id = ? AND last_updated > ?',
                                      (chat_id, current_time - timedelta(seconds=self.cache_duration)))
            row = await cursor.fetchone()
            if row:
                return dict(zip([col[0] for col in cursor.description], row))

        # 重置速率限制
        if time.time() - self.last_reset > 60:
            self.rate_limit = asyncio.Semaphore(self.rate_limit_per_minute)
            self.last_reset = time.time()

        async with self.rate_limit:
            try:
                chat = await self.bot.get_chat(chat_id)
                members_count = await chat.get_member_count()
                admins = [member.user for member in await chat.get_administrators()
                          if member.status == ChatMemberStatus.ADMINISTRATOR]

                chat_info = {
                    'chat_id': chat.id,
                    'title': chat.title,
                    'type': chat.type,
                    'members_count': members_count,
                    'admins': str([{'id': admin.id, 'username': admin.username} for admin in admins]),
                    'description': chat.description,
                    'last_updated': current_time.isoformat()
                }

                # 更新数据库
                async with aiosqlite.connect(self.db_path) as db:
                    await db.execute('''
                        INSERT OR REPLACE INTO group_info 
                        (chat_id, title, type, members_count, admins, description, last_updated) 
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', tuple(chat_info.values()))
                    await db.commit()

                return chat_info
            except TelegramError as e:
                logger.error(f"Error getting info for chat {chat_id}: {e}")
                return None

    async def get_multiple_chat_info(self, chat_ids):
        tasks = [self.get_chat_info(chat_id) for chat_id in chat_ids]
        return await asyncio.gather(*tasks)

    async def process_large_group_list(self, chat_ids, batch_size=100):
        total_groups = len(chat_ids)
        processed = 0

        for i in range(0, total_groups, batch_size):
            batch = chat_ids[i:i+batch_size]
            results = await self.get_multiple_chat_info(batch)
            processed += len(batch)
            logger.info(f"Processed {processed}/{total_groups} groups")

            # 这里可以添加进一步处理结果的逻辑
            for result in results:
                if result:
                    logger.info(f"Updated info for group: {result['title']}")
                else:
                    logger.warning("Failed to fetch info for a group")

            # 添加延迟以避免超过长期速率限制
            if processed % 1000 == 0:
                logger.info("Taking a break to avoid long-term rate limits...")
                await asyncio.sleep(60)

async def main():
    bot_token = 'YOUR_BOT_TOKEN'
    fetcher = TelegramMassGroupInfoFetcher(bot_token)
    await fetcher.init_db()

    # 假设你有一个大型的群组ID列表
    chat_ids = ['CHAT_ID_1', 'CHAT_ID_2', 'CHAT_ID_3', ...]  # 可能有成千上万个ID

    await fetcher.process_large_group_list(chat_ids)

if __name__ == "__main__":
    # asyncio.run(sendMsg())
    # asyncio.run(getBotId())
    # asyncio.run(getGroupId())
    # asyncio.run(get_chat_members())
    # get_group_members()
    # get_group_members1()
    # get_group_members2()
    # get_group_msgs('fqcollect')
    asyncio.run(get_user_info(6968043747))
    asyncio.run(get_user_info('dfkowseb'))

# https://my.telegram.org/apps  18175186559


import asyncio
import time
from telegram import Bot
from telegram.error import TelegramError
from telegram.constants import ChatMemberStatus
import aiohttp

class TelegramGroupInfoFetcher:
    def __init__(self, bot_token, cache_duration=300):
        self.bot = Bot(token=bot_token)
        self.cache = {}
        self.cache_duration = cache_duration
        self.rate_limit = asyncio.Semaphore(20)  # 限制每分钟最多处理20个不同的群组
        self.last_reset = time.time()

    async def get_chat_info(self, chat_id):
        current_time = time.time()

        # 检查缓存
        if chat_id in self.cache and current_time - self.cache[chat_id]['timestamp'] < self.cache_duration:
            return self.cache[chat_id]['data']

        # 重置速率限制
        if current_time - self.last_reset > 60:
            self.rate_limit = asyncio.Semaphore(20)
            self.last_reset = current_time

        async with self.rate_limit:
            try:
                chat = await self.bot.get_chat(chat_id)
                members_count = await chat.get_member_count()
                admins = [member.user for member in await chat.get_administrators()
                          if member.status == ChatMemberStatus.ADMINISTRATOR]

                chat_info = {
                    'id': chat.id,
                    'title': chat.title,
                    'type': chat.type,
                    'members_count': members_count,
                    'admins': [{'id': admin.id, 'username': admin.username} for admin in admins],
                    'description': chat.description
                }

                # 更新缓存
                self.cache[chat_id] = {
                    'data': chat_info,
                    'timestamp': current_time
                }

                return chat_info
            except TelegramError as e:
                print(f"Error getting info for chat {chat_id}: {e}")
                return None

    async def get_multiple_chat_info(self, chat_ids):
        tasks = [self.get_chat_info(chat_id) for chat_id in chat_ids]
        return await asyncio.gather(*tasks)

async def main():
    bot_token = 'YOUR_BOT_TOKEN'
    fetcher = TelegramGroupInfoFetcher(bot_token)

    chat_ids = ['CHAT_ID_1', 'CHAT_ID_2', 'CHAT_ID_3', ...]  # 添加你想获取信息的群组ID列表

    results = await fetcher.get_multiple_chat_info(chat_ids)

    for chat_info in results:
        if chat_info:
            print(f"Chat: {chat_info['title']}, Members: {chat_info['members_count']}")
        else:
            print("Failed to fetch chat info")

import asyncio
import os

from telethon import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest

# 166 aniu923 阿牛
api_id = '26534384'
api_hash = '8c32b586b4c83b04b9ab7d50c6464907'
# cjss_my_bot 超级阿牛
bot_token = '7699660452:AAFIm4NKhsWtpxYAyDJnWDA6ylYwHifXW4k'

# chaojianiu
user_id = '5380352615'

async def getBot():
    client = TelegramClient('mybot', api_id, api_hash, bot_token=bot_token)
    return client

def getApp():
    client = TelegramClient('myapp', api_id, api_hash)
    return client

async def get_group_info(group_username):
    client = getBot()
    client.get_chat_history()

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

async def get_group_msg(g_username):
    client = TelegramClient('fq_cj002_bot', api_id, api_hash)
    await client.start(bot_token="7699660452:AAFIm4NKhsWtpxYAyDJnWDA6ylYwHifXW4k")
    entity = await client.get_entity(g_username)
    # 获取历史消息
    messages = await client(GetHistoryRequest(
        peer=entity,
        limit=10,  # 获取的消息数量
        offset_date=None,
        offset_id=0,
        max_id=0,
        min_id=0,
        add_offset=0,
        hash=0
    ))

    # 打印消息
    for message in messages.messages:
        print(message.sender_id, ':', message.text)

def get_msg_info():
    client = getPyClient()

    async def get_group_members(group_username):
        client.start()
        group = client.get_chat_members(group_username)
        async for member in client.get_chat_members(group_username):
            print(member)

        return group

    # 填写你要采集成员的群组用户名
    group_username = 'mygroup423'

    # 采集群组成员
    members = client.loop.run_until_complete(get_group_members(group_username))

    # 打印成员信息
    data = []
    for member in members:
        data.append([member.id, member.username, member.first_name])
        print(f"用户名：{member.username}，姓名：{member.first_name} {member.last_name}，ID：{member.id}")

    # 关闭Telegram客户端
    client.disconnect()

    cur_dir = os.path.dirname(__file__)
    file_tg_url = os.path.join(cur_dir, 'users.csv')
    with open(file_tg_url, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(data)

if __name__ == "__main__":
    asyncio.run(get_group_msg("bb8o8dd"))

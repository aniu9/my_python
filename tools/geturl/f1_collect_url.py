import asyncio
import datetime
import os
import re

from telethon import TelegramClient, events
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import MessageEntityTextUrl
from tools.util import file_util

# 替换为你的API ID和API Hash
API_ID = '26534384'
API_HASH = '8c32b586b4c83b04b9ab7d50c6464907'
BOT_TOKEN = '7931792484:AAEX1QM7483FqCpcRvwFBEdbr2_Hb6uWp5s'
PHONE_NUMBER = "+85516683117"

# 创建客户端
# client = TelegramClient('collect', API_ID, API_HASH).start(bot_token=BOT_TOKEN)

# 重连延迟（秒）
RECONNECT_DELAY = 5

_cur_dir = os.path.dirname(__file__)


def msg_handler(message_text):
    # 这个正则表达式模式用于匹配URL
    url_pattern = re.compile(r'\(https://t.me/.*\)')
    urls = re.findall(url_pattern, message_text)
    if len(urls) == 0:
        return
    arrUrl = []
    for url in urls:
        arrUrl.append(url.replace("(", "").replace(")", ""))
    file = _cur_dir + "/f1_result.csv"
    file_util.write_lst_csv(arrUrl, file, 0)

    print(f"{datetime.datetime.now()},{len(urls)}")


async def bot_get_msg():
    while True:
        try:
            # 创建客户端
            client = TelegramClient('bot_collect', API_ID, API_HASH)

            # 连接并启动
            await client.start(bot_token=BOT_TOKEN)

            # 注册消息处理器
            @client.on(events.NewMessage)
            async def handler(event):
                # 获取消息文本
                message_text = event.message.text
                msg_handler(message_text)

            print("机器人已启动并正在监听消息...")

            # 保持客户端运行
            await client.run_until_disconnected()
        except Exception as e:
            print(f"发生错误: {str(e)}")
            print(f"{RECONNECT_DELAY} 秒后尝试重新连接...")
            await asyncio.sleep(RECONNECT_DELAY)
        finally:
            # 确保客户端正确断开连接
            if client.is_connected():
                await client.disconnect()


def app_get_msgs():
    configFile = _cur_dir + "/f1_config.csv"
    arr_max_id = file_util.read_first_csv(configFile)

    client = TelegramClient('app_collect', API_ID, API_HASH)
    # group_username = 'fqcollect'
    group_username1 = 'enSearchGroup'
    group_username2 = 'enSearchBot_1'

    async def get_msgs(group_name, last_max_id):
        await client.start()
        # 确保完全授权
        if not await client.is_user_authorized():
            await client.send_code_request(PHONE_NUMBER)
            try:
                await client.sign_in(PHONE_NUMBER, input('Enter the code: '))
            except:
                await client.sign_in(password=input('Password: '))

        # 获取群组实体
        entity = await client.get_entity(group_name)
        limit = 100
        offset_id = 0
        total_msg = 0
        total_count_limit = 10000  # 设置为 0 表示获取所有消息
        if not last_max_id:
            last_max_id = 0
        min_id = last_max_id
        # datetime.datetime.now(pytz.timezone('Asia/Shanghai'))
        # today_start = datetime.datetime.now(pytz.utc).replace(hour=0, minute=0, second=0, microsecond=0)

        # 获取历史消息
        while True:
            messages = await client(GetHistoryRequest(
                peer=entity,
                limit=limit,  # 获取的消息数量
                offset_date=None,
                offset_id=offset_id,
                max_id=0,
                min_id=min_id,
                add_offset=0,
                hash=0
            ))
            msgs = messages.messages
            if not msgs:
                break
            if msgs[0].id > last_max_id:
                last_max_id = msgs[0].id
            offset_id = msgs[-1].id
            total_msg = total_msg + len(msgs)

            for msg in msgs:
                if not isinstance(msg.entities, list):
                    continue
                for item in msg.entities:
                    if isinstance(item, MessageEntityTextUrl):
                        msg_handler(f"({item.url})")

            if total_count_limit != 0 and total_msg >= total_count_limit:
                break
            if len(msgs) < limit:
                # 到达频道的开始
                break
        return last_max_id

    last_max_id = client.loop.run_until_complete(get_msgs(group_username1, arr_max_id[0]))
    print(f"{group_username1},last_max_id: {last_max_id}")
    arr_max_id[0] = last_max_id

    last_max_id = client.loop.run_until_complete(get_msgs(group_username2, arr_max_id[1]))
    print(f"{group_username2},last_max_id: {last_max_id}")
    arr_max_id[1] = last_max_id

    file_util.write_lst_csv(arr_max_id, configFile, 1)
    # 关闭Telegram客户端
    client.disconnect()


if __name__ == '__main__':
    # file = file_util.get_file_path("f4_result.csv")
    # file_util.empty_csv(file)
    # 运行机器人
    # asyncio.run(bot_get_msg())
    app_get_msgs()

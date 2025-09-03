import asyncio
import time
from pyrogram import Client, errors
from pyrogram.raw import functions, types
import uvloop
import random

# 启用uvloop提升异步性能 (可选但推荐)
uvloop.install()

class MessageSender:
    def __init__(self, api_id, api_hash, bot_token=None, session_name="mass_bot"):
        """
        初始化群发机器人

        参数:
            api_id (int): Telegram API ID
            api_hash (str): Telegram API Hash
            bot_token (str): 机器人token (可选)
            session_name (str): 会话名称
        """
        self.client = Client(session_name, api_id=api_id, api_hash=api_hash, bot_token=bot_token)
        self.semaphore = asyncio.Semaphore(20)  # 控制并发量
        self.sent_count = 0
        self.start_time = None

    async def send_single_message(self, chat_id, text, max_retries=3):
        """
        发送单条消息，带有重试机制
        """
        async with self.semaphore:
            for attempt in range(max_retries):
                try:
                    # 使用原始API提高性能 :cite[2]:cite[7]
                    peer = await self.client.resolve_peer(chat_id)
                    random_id = random.randint(0, 0x7fffffff)  # 生成随机ID

                    await self.client.invoke(
                        functions.messages.SendMessage(
                            peer=peer,
                            message=text,
                            random_id=random_id,
                            no_webpage=True  # 禁用网页预览加速发送
                        )
                    )

                    self.sent_count += 1
                    elapsed = time.time() - self.start_time

                    # 每秒打印进度
                    if self.sent_count % 10 == 0:
                        print(f"已发送: {self.sent_count} | 速率: {self.sent_count/elapsed:.2f}条/秒")

                    return True

                except errors.FloodWait as e:
                    # Telegram限制等待
                    wait_time = e.value + 5  # 额外增加5秒缓冲
                    print(f"遇到限制，等待 {wait_time} 秒")
                    await asyncio.sleep(wait_time)

                except (errors.PeerIdInvalid, errors.ChannelInvalid):
                    print(f"无效的聊天ID: {chat_id}")
                    return False

                except Exception as e:
                    print(f"发送到 {chat_id} 失败 (尝试 {attempt+1}/{max_retries}): {str(e)}")
                    await asyncio.sleep(2 ** attempt)  # 指数退避

        return False

    async def send_bulk_messages(self, chat_ids, text, delay=0.05):
        """
        批量发送消息到多个聊天

        参数:
            chat_ids (list): 聊天ID列表
            text (str): 要发送的文本
            delay (float): 发送间隔(秒)
        """
        self.start_time = time.time()
        self.sent_count = 0

        tasks = []
        for chat_id in chat_ids:
            task = asyncio.create_task(self.send_single_message(chat_id, text))
            tasks.append(task)

            # 控制发起速率
            await asyncio.sleep(delay)

        # 等待所有任务完成
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 统计结果
        successful = sum(1 for r in results if r is True)
        failed = len(results) - successful

        total_time = time.time() - self.start_time
        print(f"\n发送完成!")
        print(f"成功: {successful}, 失败: {failed}")
        print(f"总用时: {total_time:.2f}秒")
        print(f"平均速率: {successful/total_time:.2f}条/秒")

        return successful, failed

    async def run(self, chat_ids, message_text):
        """
        运行群发任务
        """
        async with self.client:
            # 验证身份
            me = await self.client.get_me()
            print(f"登录成功: {me.first_name} (@{me.username})")

            # 开始发送
            return await self.send_bulk_messages(chat_ids, message_text)

# 使用示例
async def main():
    # 配置参数
    API_ID = 1234567  # 替换为你的API ID
    API_HASH = "your_api_hash_here"  # 替换为你的API Hash
    BOT_TOKEN = "your_bot_token_here"  # 或用用户session

    # 要发送的聊天ID列表 (可以是用户ID、群组ID或频道ID)
    CHAT_IDS = [
        123456789,    # 用户ID
        -100123456789, # 频道或群组ID
        # ...添加更多ID
    ]

    MESSAGE_TEXT = "您好！这是一条测试消息。感谢您的关注！"

    # 创建发送器实例
    sender = MessageSender(API_ID, API_HASH, bot_token=BOT_TOKEN)

    # 开始发送
    successful, failed = await sender.run(CHAT_IDS, MESSAGE_TEXT)

    print(f"\n结果: 成功发送 {successful} 条消息, {failed} 条失败")

if __name__ == "__main__":
    asyncio.run(main())
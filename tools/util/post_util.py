import asyncio
import logging
import random
import time
from datetime import datetime
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import FloodWait, PeerIdInvalid, ChannelInvalid
import json
import os

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MultiBotBroadcaster:
    def __init__(self, config_file="config.json"):
        self.config_file = config_file
        self.bots = []
        self.target_chats = []
        self.current_bot_index = 0
        self.sent_count = 0
        self.failed_count = 0
        self.start_time = None

    def load_config(self):
        """加载配置文件"""
        if not os.path.exists(self.config_file):
            # 创建默认配置模板
            default_config = {
                "bot_tokens": [
                    "bot_token_1",
                    "bot_token_2",
                    # 添加更多机器人token
                ],
                "api_id": 1234567,
                "api_hash": "your_api_hash_here",
                "target_chats": [
                    -1001234567890,  # 频道/群组ID
                    -1000987654321   # 另一个频道/群组ID
                ],
                "delay_range": [1, 3],  # 消息之间的延迟范围（秒）
                "max_messages_per_minute": 1000  # 目标发送速率
            }

            with open(self.config_file, "w") as f:
                json.dump(default_config, f, indent=4)

            logger.error(f"请编辑配置文件: {self.config_file}")
            return False

        with open(self.config_file, "r") as f:
            config = json.load(f)

        self.bot_tokens = config["bot_tokens"]
        self.api_id = config["api_id"]
        self.api_hash = config["api_hash"]
        self.target_chats = config["target_chats"]
        self.delay_range = config["delay_range"]
        self.max_messages_per_minute = config["max_messages_per_minute"]

        return True

    async def initialize_bots(self):
        """初始化所有机器人客户端"""
        for i, token in enumerate(self.bot_tokens):
            try:
                bot = Client(
                    f"bot_{i}",
                    api_id=self.api_id,
                    api_hash=self.api_hash,
                    bot_token=token
                )

                await bot.start()
                self.bots.append(bot)
                logger.info(f"机器人 {i} 初始化成功")

            except Exception as e:
                logger.error(f"机器人 {i} 初始化失败: {e}")

        return len(self.bots) > 0

    def get_next_bot(self):
        """获取下一个可用的机器人（轮询负载均衡）"""
        if not self.bots:
            return None

        bot = self.bots[self.current_bot_index]
        self.current_bot_index = (self.current_bot_index + 1) % len(self.bots)
        return bot

    async def send_message(self, chat_id, text):
        """使用一个机器人发送消息"""
        bot = self.get_next_bot()
        if not bot:
            logger.error("没有可用的机器人")
            return False

        try:
            await bot.send_message(chat_id, text)
            self.sent_count += 1
            return True

        except FloodWait as e:
            logger.warning(f"机器人遇到 FloodWait，需要等待 {e.value} 秒")
            await asyncio.sleep(e.value)
            # 重试发送
            return await self.send_message(chat_id, text)

        except (PeerIdInvalid, ChannelInvalid) as e:
            logger.error(f"无法发送到 {chat_id}: {e}")
            self.failed_count += 1
            return False

        except Exception as e:
            logger.error(f"发送消息时出错: {e}")
            self.failed_count += 1
            return False

    async def broadcast(self, message_text):
        """广播消息到所有目标聊天"""
        if not self.bots:
            logger.error("没有可用的机器人")
            return

        self.start_time = datetime.now()
        self.sent_count = 0
        self.failed_count = 0

        total_chats = len(self.target_chats)
        logger.info(f"开始向 {total_chats} 个聊天发送消息")

        for i, chat_id in enumerate(self.target_chats):
            success = await self.send_message(chat_id, message_text)

            # 计算进度
            progress = (i + 1) / total_chats * 100
            elapsed = (datetime.now() - self.start_time).total_seconds()

            if elapsed > 0:
                speed = self.sent_count / elapsed  # 消息/秒
                eta = (total_chats - i - 1) / speed if speed > 0 else 0
            else:
                speed = 0
                eta = 0

            logger.info(
                f"进度: {progress:.1f}% | "
                f"已发送: {self.sent_count} | "
                f"失败: {self.failed_count} | "
                f"速度: {speed:.2f} 条/秒 | "
                f"预计剩余: {eta:.1f} 秒"
            )

            # 添加随机延迟以避免触发限制
            delay = random.uniform(self.delay_range[0], self.delay_range[1])
            await asyncio.sleep(delay)

        # 打印最终报告
        elapsed_total = (datetime.now() - self.start_time).total_seconds()
        logger.info(
            f"广播完成! "
            f"总计: {total_chats} | "
            f"成功: {self.sent_count} | "
            f"失败: {self.failed_count} | "
            f"总耗时: {elapsed_total:.1f} 秒 | "
            f"平均速度: {self.sent_count/elapsed_total:.2f} 条/秒"
        )

    async def stop_all_bots(self):
        """停止所有机器人"""
        for bot in self.bots:
            try:
                await bot.stop()
            except:
                pass

        self.bots = []
        logger.info("所有机器人已停止")

async def main():
    """主函数"""
    broadcaster = MultiBotBroadcaster()

    # 加载配置
    if not broadcaster.load_config():
        return

    # 初始化机器人
    if not await broadcaster.initialize_bots():
        return

    try:
        # 获取要发送的消息
        message_text = input("请输入要广播的消息: ")

        # 开始广播
        await broadcaster.broadcast(message_text)

    except KeyboardInterrupt:
        logger.info("用户中断操作")

    finally:
        # 清理资源
        await broadcaster.stop_all_bots()

if __name__ == "__main__":
    asyncio.run(main())
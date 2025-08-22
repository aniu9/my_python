from tenacity import retry, stop_after_attempt, wait_fixed
from tortoise import Tortoise

# 启用Tortoise的查询日志
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("tortoise")
logger.setLevel(logging.DEBUG)

@retry(stop=stop_after_attempt(3), wait=wait_fixed(3))
async def init_db():
    await Tortoise.init(
        {
            # 多个数据库
            "connections": {
                "default": {
                    "engine": "tortoise.backends.mysql",
                    "credentials": {
                        "database": "cms",
                        "host": "localhost",
                        "port": 13306,
                        "user": "admin",
                        "password": "pwrdscjss2w3e5t",
                        "maxsize": 5,
                        "connect_timeout": 10,
                    }
                }
            },
            # 可以指定多个不同的models
            "apps": {
                "models": {
                    "models": ["tools.model.entities"],
                    "default_connection": "default",
                }
            },
            'use_tz': False,
            'timezone': 'Asia/Shanghai'
        }
    )

async def close_db():
    """关闭数据库连接"""
    await Tortoise.close_connections()

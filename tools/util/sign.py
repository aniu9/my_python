import asyncio
import hashlib
import json
from datetime import datetime

import requests

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


def sendHeaderGenerate(data):
    # 暂时固定给的都是1
    SERVER_APP_KEY = "2"
    now = datetime.now()
    # 将 datetime 对象转换为时间戳
    timestamp = int(now.timestamp() * 1000)
    # body参数
    j = json.dumps(data)
    s = sign(str(SERVER_APP_KEY), timestamp, j)
    headers = {
        'Content-Type': 'application/json',
        "appkey": str(SERVER_APP_KEY),
        "timestamp": str(timestamp),
        "sign": s
    }
    return headers

async def request():
    url = 'http://a276b8d3ca3a14befa1dc6335eaa47ea-f83cb44aa303c283.elb.ap-southeast-1.amazonaws.com:8800/botmng-service/cms/filter/getFilters'
    # url = 'http://localhost:8083/botmng-service/cms/filter/getFilters'
    params={
        "platType": "feibo",
        "langCode": "zh-cn"
    }
    headers = sendHeaderGenerate(params)
    response = requests.post(url, json=params, headers=headers)
    print(response.text)

if __name__ == "__main__":
    asyncio.run(request())
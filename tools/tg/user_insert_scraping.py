import random

ENV_LOGGER_CHAT = -1002404224371
ENV_BOT_TOKEN = "8192285900:AAEdh_6ESUPio1AM5FAphti6hUF55X1od_g"

api_id = 20079525
api_hash = "3365973876c9cab387e0629d690fbe19"
TOKENS = [
    {
        "bot_name": "cjan_001_bot", "token": "7908663209:AAGC4CVFr5NRio9mOP8tQfqqaorirhPzzT0"
    },
    {
        "bot_name": "cjan_002_bot", "token": "7781291686:AAH3L0XGwzkhtJvyeJzhC-Dy2D71NVu-a3k"
    },
    # {
    #     "bot_name": "cjss_cj002_bot", "token": "7815275035:AAG3EPj39IommOSpS6AW_uNEcYH3uRGwJcs"
    # },
    # {
    #     "bot_name": "cjss_cj003_bot", "token": "7636152888:AAHj1w2GgOhSAA6WXeSjtdZFwj9e054s6xM"
    # }
]
# 每条链接休眠多少秒
per_link_sleep = 4
ONE_TIME_CMS_FETCH_COUNT = 20
LINK_VERIFY_COOLDOWN = 6

import asyncio
import json
import re
import requests
import time
from datetime import datetime, timedelta

import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from pyrogram import Client, errors, enums, types
from telethon import TelegramClient
from telethon.tl.functions.channels import GetFullChannelRequest

class AwsS3Bucket:
    lambda_url = "https://wkplvul4qc4p45lox3khamzpeq0jziab.lambda-url.ap-southeast-1.on.aws/"

    def upload_file(self, data: str, retry=0):
        """
        使用预签名 URL 上传文件
        """
        try:
            now = datetime.now(tz=pytz.timezone("Asia/Shanghai"))
            name = now.strftime("%Y%m%d%H%M%S") + ".json"
            generateFileName = "cms/collect/" + now.strftime("%Y/%m/%d/%H") + f"/group_u_{name}"

            localTmpFilePath = "./user_insert_empty.json"

            result = self.get_presigned_url(generateFileName)
            presigned_url = result['presigned_url']

            # # 上传文件
            with open(localTmpFilePath, 'w+') as file:
                file.flush()
                file.seek(0)
                file.write(data)
            with open(localTmpFilePath, 'rb') as file:
                headers = {'Content-Type': "application/json"}
                response = requests.put(presigned_url, data=file, headers=headers)

                if response.status_code == 200:
                    print(f"File uploaded successfully to {result['bucket_name']}/{result['file_name']}")
                else:
                    print(f"Failed to upload file. Status code: {response.status_code}")
                    print(f"Response: {response.text}")
        except Exception as e:
            cms.loggerEvent(f"Bucket insertion fail {retry + 1} times: {str(e)}")
            time.sleep(1)
            retry += 1
            if retry == 4:
                cms.loggerEvent(f"Bucket insertion without retry: {str(e)}")
                return
            self.upload_file(data=data, retry=retry)

    def get_presigned_url(self, filename):
        """
        调用 Lambda 函数获取预签名 URL
        """
        params = {
            'filename': filename,
            'contentType': "application/json"
        }
        response = requests.get(self.lambda_url, params=params)
        if response.status_code == 200:
            return json.loads(response.text)
        else:
            raise Exception(f"Failed to get presigned URL: {response.text}")

class CMSService:
    bucket = AwsS3Bucket()
    _prefix = "http://a276b8d3ca3a14befa1dc6335eaa47ea-f83cb44aa303c283.elb.ap-southeast-1.amazonaws.com:8800/cms-service/cms"
    __headers = {
        "gateway_internal_request_flag": "dhasjkdhaskgvhcbxahsa",
        "appKey": "065b4d200b6511efa666069f84b557b",
        "User-Agent": "Apifox/1.0.0 (https://apifox.com)",
        "Content-Type": "application/json"
    }

    def getLinkToVerify(self):
        url = self._prefix + "/content/getEmptyLinks"
        payload = {
            "pageSize": ONE_TIME_CMS_FETCH_COUNT
        }
        response = requests.post(url, headers=self.__headers, data=json.dumps(payload))
        js = response.json()
        print(js)
        if js["code"] == 200:
            return [link["url"] for link in js["data"]]
        return []

    def collectGroupInfo(self, payload: list[dict]):
        re = self.bucket.upload_file(data=json.dumps(payload))

        # url = self._prefix + "/content/collectGroupInfo"
        # response = requests.post(url, headers= self.__headers, data= json.dumps(payload))
        # js = response.json()
        # return js

    def loggerEvent(self, text: str):
        try:
            url = f"https://api.telegram.org/bot{ENV_BOT_TOKEN}/sendMessage"
            payload = {
                "chat_id": "-1002404224371",
                "text": text,
            }
            response = requests.post(url, json=payload)
            if response.status_code != 200:
                print("Http log error ", str(response.text))
        except Exception as e:
            print("Http log error ", str(e))

cms = CMSService()

async def logicCode(bot_name: str, client: Client):
    urls = cms.getLinkToVerify()
    print(f"Len {len(urls)} to verify....")
    cmsPayloads = []
    for ind, url in enumerate(urls):
        pattern = r'https:\/\/t.me\/(.*)'
        matching = re.match(pattern=pattern, string=url)
        if not matching: continue
        username = matching.groups()[0]
        try:
            print(f"{bot_name} -> Fetching: {username} of {ind + 1}/{len(urls)}")
            chat = await client.get_chat(username, force_full=True)
            if chat.type == enums.ChatType.CHANNEL:
                chat_type = 1
            elif chat.type == enums.ChatType.GROUP or chat.type == enums.ChatType.SUPERGROUP:
                chat_type = 0
            else:
                raise ValueError(f"Not a group or channel url: {url}")
            try:
                members = [member async for member in
                           client.get_chat_members(chat_id=username, filter=enums.ChatMembersFilter.ADMINISTRATORS)]
            except Exception as e:
                members = []
                print(f"拿不到Admin {str(e)}")
            cmsPayloads.append({
                "isValid": 1,  # 0 无效 1有效
                "url": url,
                "tgId": chat.id,
                "linkType": chat_type,
                "userNum": chat.members_count,
                "userName": chat.username or '',
                "title": chat.title,
                "description": chat.description or '',
                "admins": [{"tgUserId": member.user.id, "tgUserName": member.user.username or ''} for member in members]
            })
        except (errors.PeerIdInvalid, errors.UsernameNotOccupied, ValueError) as e:
            cmsPayloads.append({
                "isValid": 0,  # 0 无效 1有效
                "url": url,
            })
        except Exception as e:
            if 'FLOOD_WAIT' in str(e):
                cms.loggerEvent(f"{bot_name} 被封控 {e.value}秒。")
                return cmsPayloads
            cms.loggerEvent("Error getting record" + str(e))
        time.sleep(ONE_TIME_CMS_FETCH_COUNT / (LINK_VERIFY_COOLDOWN * 1.5))
    return cmsPayloads

async def busineseRequirement():
    try:
        bot_session = TOKENS.pop()
        bot_name = bot_session["bot_name"]
        async with Client(
                name=bot_name,
                api_id=20079525,
                api_hash="3365973876c9cab387e0629d690fbe19",
                bot_token=bot_session["token"],
        ) as client:
            payload = await logicCode(bot_name=bot_name, client=client)
            valid = len([i for i in payload if i["isValid"] == 1])
            inValid = len([i for i in payload if i["isValid"] == 0])
            if valid == 0 and inValid == 0:
                cms.loggerEvent("Valid: 0, Invalid :0")
                return
            save = cms.collectGroupInfo(payload)
            print(save)
            logText = ""
            if save["code"] == 200:
                logText = f"""
Bot: {bot_name}
Valid: {valid}
InValid: {inValid}
"""
            else:
                logText = str(save)

            cms.loggerEvent(logText)
            await client.stop()
    except Exception as e:
        print("Exception out")
        print(e)
    finally:
        TOKENS.insert(0, bot_session)

async def busineseRequirement2():
    # 注册客户端池
    clients = []
    for token in TOKENS:
        client = Client(name=token["bot_name"], api_id=api_id, api_hash=api_hash, bot_token=token["token"])
        await client.start()
        clients.append({"tokenName": token["bot_name"], "client": client})

    time0 = datetime.now()
    total_cnt = 0
    err_msg = ""
    # sleep_time = datetime.now() + timedelta(minutes=30)
    # 循环执行
    while err_msg == "":
        try:
            # 每执行30分钟休息30分钟
            # if datetime.now() >= sleep_time:
            #     time.sleep(1800)
            #     sleep_time = datetime.now() + timedelta(minutes=30)

            # 获取待查询链接
            urls = cms.getLinkToVerify()
            # 无待查询链接休眠10分钟
            if len(urls) == 0:
                print(f"{datetime.now()}, no data, sleep 10 minutes")
                time.sleep(600)
                continue

            payload = []
            start_time = datetime.now()
            for ind, url in enumerate(urls):
                pattern = r'https:\/\/t.me\/(.*)'
                matching = re.match(pattern=pattern, string=url)
                if not matching: continue
                username = matching.groups()[0]
                try:
                    clientObj = clients.pop()
                    client = clientObj["client"]
                    token_name = clientObj["tokenName"]

                    # 获取信息
                    total_cnt += 1
                    chat = await client.get_chat(username)
                    if chat.type == enums.ChatType.CHANNEL:
                        chat_type = 1
                    elif chat.type == enums.ChatType.GROUP or chat.type == enums.ChatType.SUPERGROUP:
                        chat_type = 0
                    else:
                        payload.append({"isValid": 0, "url": url})
                        continue

                    # 待发送数据
                    payload.append({
                        "isValid": 1,  # 0 无效 1有效
                        "url": url,
                        "tgId": chat.id,
                        "linkType": chat_type,
                        "userNum": chat.members_count,
                        "userName": username,
                        "title": chat.title,
                        "description": chat.description
                    })
                # except errors.UsernameInvalid as e:
                #     success_cnt += 1
                #     payload.append({"isValid": 0, "url": url})
                except errors.FloodWait as e:
                    # print(f"{datetime.now()}, {token_name}, Waiting for {e.value} seconds.")
                    # await asyncio.sleep(e.value)
                    err_msg = f"{token_name}: Waiting for {e.value} seconds."
                    break
                except Exception as e:
                    # err_msg = str(e)
                    # break
                    print(f"{datetime.now()}, {token_name}, url:{url}, error: {e}")
                finally:
                    clients.insert(0, clientObj)
                    # num = round(15 / len(clients), 1)
                    # num = random.randrange(int(num * 0.6 * 100), int(num * 1.4 * 100)) / 100
                    time.sleep(per_link_sleep)
            duration = datetime.now() - start_time
            print(f"{datetime.now()}, cost:{duration}, successCnt:{len(payload)}, clientsCnt:{len(clients)}")
            # print(f"{payload}")
            if len(payload) > 0:
                cms.collectGroupInfo(payload)
        except Exception as e:
            print(f"{datetime.now()}, Exception out:{e}")
    # 异常退出循环，终止程序
    duration = datetime.now() - time0
    print(f'{datetime.now()}, totalCost:{duration}, totalCnt:{total_cnt}, clientsCnt:{len(clients)}, error:{err_msg}')
    for client in clients:
        await client["client"].stop()

async def busineseRequirement21():
    api_id = 20079525
    api_hash = "3365973876c9cab387e0629d690fbe19"
    clients = []
    for token in TOKENS:
        client = Client(name=token["bot_name"], api_id=api_id, api_hash=api_hash, bot_token=token["token"])
        await client.start()
        clients.append({"tokenName": token["bot_name"], "client": client})

    time0 = datetime.now()
    total_cnt = 0
    err_msg = ""
    sleep_time = datetime.now() + timedelta(minutes=random.randrange(12, 28))
    while err_msg == "":
        try:
            if datetime.now() >= sleep_time:
                time.sleep(random.randrange(6, 14))
                sleep_time = datetime.now() + timedelta(minutes=random.randrange(12, 28))
            urls = cms.getLinkToVerify()
            if len(urls) == 0:
                print(f"{datetime.now()}, no data, sleep 10 minutes")
                time.sleep(600)
            payload = []
            start_time = datetime.now()
            success_cnt = 0
            for ind, url in enumerate(urls):
                pattern = r'https:\/\/t.me\/(.*)'
                matching = re.match(pattern=pattern, string=url)
                if not matching: continue
                username = matching.groups()[0]
                try:
                    clientObj = clients.pop()
                    client = clientObj["client"]
                    token_name = clientObj["tokenName"]
                    total_cnt += 1
                    chat = await client.get_chat(username)
                    if chat.type == enums.ChatType.CHANNEL:
                        chat_type = 1
                    elif chat.type == enums.ChatType.GROUP or chat.type == enums.ChatType.SUPERGROUP:
                        chat_type = 0
                    else:
                        raise ValueError(f"Not a group or channel url: {url}")
                    success_cnt += 1
                    payload.append({
                        "isValid": 1,  # 0 无效 1有效
                        "url": url,
                        "tgId": chat.id,
                        "linkType": chat_type,
                        "userNum": chat.members_count,
                        "userName": username,
                        "title": chat.title,
                        "description": chat.description
                    })
                # except errors.UsernameInvalid as e:
                #     success_cnt += 1
                #     payload.append({"isValid": 0, "url": url})
                except errors.FloodWait as e:
                    # print(f"{datetime.now()}, {token_name}, Waiting for {e.value} seconds.")
                    # await asyncio.sleep(e.value)
                    err_msg = f"{token_name}: Waiting for {e.value} seconds."
                    break
                except Exception as e:
                    # err_msg = str(e)
                    # break
                    print(f"{datetime.now()}, tokenName:{token_name}, url:{url}, error: {e}")
                finally:
                    clients.insert(0, clientObj)
                    num = round(15 / len(clients), 1)
                    num = random.randrange(int(num * 0.6 * 100), int(num * 1.4 * 100)) / 100
                    time.sleep(num)
            duration = datetime.now() - start_time
            print(f"{datetime.now()}, cost:{duration}, success_cnt:{success_cnt}")
            # print(f"{payload}")
            if len(payload) > 0:
                cms.collectGroupInfo(payload)
        except Exception as e:
            print(f"{datetime.now()}, Exception out:{e}")
    duration = datetime.now() - time0
    print(f'{datetime.now()} end, totalCost:{duration}, totalCnt:{total_cnt}, msg:{err_msg}')
    for client in clients:
        await client["client"].stop()

async def busineseRequirement3(token):
    try:
        err_msg = ""
        time0 = datetime.now()
        total_cnt = 0
        token_name = token["bot_name"]
        print(f'{datetime.now()},start:{token_name}')
        async with Client(
                name=token_name,
                api_id=20079525,
                api_hash="3365973876c9cab387e0629d690fbe19",
                bot_token=token["token"]
        ) as client:
            while err_msg == "":
                try:
                    urls = cms.getLinkToVerify()
                    if len(urls) == 0:
                        print(f"{datetime.now()},no data，sleep 10 minutes")
                        time.sleep(600)
                    payload = []
                    start_time = datetime.now()
                    success_cnt = 0
                    for ind, url in enumerate(urls):
                        pattern = r'https:\/\/t.me\/(.*)'
                        matching = re.match(pattern=pattern, string=url)
                        if not matching: continue
                        username = matching.groups()[0]
                        try:
                            chat = await client.get_chat(username)
                            if chat.type == enums.ChatType.CHANNEL:
                                chat_type = 1
                            elif chat.type == enums.ChatType.GROUP or chat.type == enums.ChatType.SUPERGROUP:
                                chat_type = 0
                            else:
                                raise ValueError(f"Not a group or channel url: {url}")
                            success_cnt += 1
                            payload.append({
                                "isValid": 1,  # 0 无效 1有效
                                "url": url,
                                "tgId": chat.id,
                                "linkType": chat_type,
                                "userNum": chat.members_count,
                                "userName": username,
                                "title": chat.title,
                                "description": chat.description
                            })
                        # except errors.UsernameInvalid as e:
                        #     success_cnt += 1
                        #     payload.append({"isValid": 0, "url": url})
                        except errors.FloodWait as e:
                            # print(f"Rate limit exceeded. Waiting for {e.x} seconds.")
                            # await asyncio.sleep(e.value)
                            user_name = (await client.get_me()).username
                            err_msg = f"{datetime.now()},userName:{user_name},error:Rate limit exceeded. Waiting for {e.value} seconds"
                            break
                        except Exception as e:
                            # err_msg = str(e)
                            # break
                            print(f"url:{url}, error: {e}")
                        finally:
                            time.sleep(random.randrange(6, 10))
                    duration = datetime.now() - start_time
                    print(f"{datetime.now()},cost:{duration},success_cnt:{success_cnt}")
                    # print(f"{payload}")
                    cms.collectGroupInfo(payload)
                except Exception as e:
                    print(f"Exception out:{e}")
    except Exception as e:
        err_msg = str(e)
    finally:
        duration = datetime.now() - time0
        print(f'{datetime.now()},stop:{token_name},totalCost:{duration},totalCnt:{total_cnt},msg:{err_msg}')

async def busineseRequirement4():
    api_id = 20079525
    api_hash = "3365973876c9cab387e0629d690fbe19"
    client = TelegramClient('fq_cj001_bot', api_id, api_hash)
    await client.start(bot_token="7543527702:AAFsPCyT9Wv4_YxuHT0i8IzNz2s1i4Mr6JU")
    time0 = datetime.now()
    total_cnt = 0
    err_msg = ""
    while err_msg == "":
        try:
            urls = cms.getLinkToVerify()
            if len(urls) == 0:
                print(f"{datetime.now()},no data，sleep 10 minutes")
                time.sleep(600)
            payload = []
            start_time = datetime.now()
            success_cnt = 0
            for ind, url in enumerate(urls):
                pattern = r'https:\/\/t.me\/(.*)'
                matching = re.match(pattern=pattern, string=url)
                if not matching: continue
                username = matching.groups()[0]
                # username="-1002212441719"
                try:
                    chat = await client.get_entity(username)

                    # 获取聊天类型
                    if chat.broadcast:
                        chat_type = 1
                    elif chat.megagroup:
                        chat_type = 0
                    elif chat.gigagroup:
                        chat_type = 0
                    elif chat.group:
                        chat_type = 0
                    else:
                        raise ValueError(f"Not a group or channel url: {url}")

                    # 获取完整信息
                    full_info = (await client(GetFullChannelRequest(channel=chat))).full_chat
                    # 获取聊天成员数量
                    members_count = None
                    if hasattr(full_info, 'participants_count'):
                        members_count = full_info.participants_count
                    # 获取描述
                    description = None
                    if hasattr(full_info, 'about'):
                        description = full_info.about

                    success_cnt += 1
                    payload.append({
                        "isValid": 1,  # 0 无效 1有效
                        "url": url,
                        "tgId": chat.id,
                        "linkType": chat_type,
                        "userNum": members_count,
                        "userName": username,
                        "title": chat.title,
                        "description": description
                    })
                # except errors.UsernameInvalid as e:
                #     success_cnt += 1
                #     payload.append({"isValid": 0, "url": url})
                except errors.FloodWait as e:
                    # print(f"Rate limit exceeded. Waiting for {e.x} seconds.")
                    # await asyncio.sleep(e.value)
                    user_name = (await client.get_me()).username
                    err_msg = f"{datetime.now()},userName:{user_name},error:Rate limit exceeded. Waiting for {e.value} seconds"
                    break
                except Exception as e:
                    # err_msg = str(e)
                    # break
                    print(f"url:{url}, error: {e}")
                finally:
                    time.sleep(random.randrange(6, 10))
            duration = datetime.now() - start_time
            print(f"{datetime.now()},cost:{duration},success_cnt:{success_cnt}")
            # print(f"{payload}")
            cms.collectGroupInfo(payload)
        except Exception as e:
            print(f"Exception out:{e}")
    duration = datetime.now() - time0
    print(f'{datetime.now()} end,totalCost:{duration},totalCnt:{total_cnt},error:{err_msg}')

def run_scheduler():
    # Create event loop
    loop = asyncio.get_event_loop()

    # Create the scheduler
    scheduler = AsyncIOScheduler()

    # Add the job
    trigger = CronTrigger(
        year="*", month="*", day="*", hour="*", minute="*", second="0"
    )

    job = scheduler.add_job(
        busineseRequirement,
        trigger=trigger,
        name="scraping",
        misfire_grace_time=5,
        coalesce=True
    )

    # Start the scheduler
    scheduler.start()

    try:
        # Run the event loop
        loop.run_forever()
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
    finally:
        loop.close()

# bot = Client(
#     name= "bot_recommendation_bot",
#     api_id=20079525,
#     api_hash="3365973876c9cab387e0629d690fbe19",
#     bot_token= "7841760620:AAHVUerwuf5ueG0fVNZLAeo_MvBcBm5bwsM"
# )
# async def main():
#     print("Running")
#     async with bot:
#         bot.add_handler(handlers.MessageHandler(onNewMessage))
#         await idle()
#         await bot.stop()

#     # run_scheduler()

async def main():
    # run_scheduler()
    # await busineseRequirement4()
    # try:
    #     tasks = []
    #     for token in TOKENS:
    #         tasks.append(busineseRequirement3(token))
    #     await asyncio.gather(*tasks)
    # except Exception as e:
    #     print(f"busineseRequirement3 error: {e}")

    try:
        await busineseRequirement2()
    except Exception as e:
        print(f"{datetime.now()},busineseRequirement2 error: {e}")

async def onNewMessage(client: Client, msg: types.Message):
    if 7767833079 == msg.from_user.id:
        await busineseRequirement()

if __name__ == "__main__":
    # bot.run(main())
    print("RUNNING")
    asyncio.run(main())

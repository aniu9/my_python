import random

ENV_LOGGER_CHAT = -1002404224371
ENV_BOT_TOKEN = "8192285900:AAEdh_6ESUPio1AM5FAphti6hUF55X1od_g"

TOKENS = [
    # {
    #     "bot_name": "cjss_test01_bot", "token": "8096915404:AAHcnrQqmgNYEr8r0OVij5X7dGFcGOwxlaM"
    # },
    # {
    #     "bot_name": "cjan_001_bot", "token": "7908663209:AAGC4CVFr5NRio9mOP8tQfqqaorirhPzzT0"
    # },
    {
        "bot_name": "fq_collect_bot", "token": "7931792484:AAEX1QM7483FqCpcRvwFBEdbr2_Hb6uWp5s"
    },
    {
        "bot_name": "cjss_my_bot", "token": "7578200869:AAGYOb4_k3LiYDEymY8FfK_X2spdnOw0DAI"
    }
]

# 每条链接休眠多少秒
per_link_sleep = 4
# 多少秒切换一次账号
change_client_period = 4
# 一个账号每天最多处理多少条链接
client_day_count = 0
# 一次拉取多少条数据
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
from pyrogram import Client, errors, types

class AwsS3Bucket:
    lambda_url = "https://wkplvul4qc4p45lox3khamzpeq0jziab.lambda-url.ap-southeast-1.on.aws/"

    def upload_file(self, data: str, retry=0):
        """
        使用预签名 URL 上传文件
        """
        try:
            now = datetime.now(tz=pytz.timezone("Asia/Shanghai"))
            name = now.strftime("%Y%m%d%H%M%S") + ".json"
            generateFileName = "cms/collect/" + now.strftime("%Y/%m/%d/%H") + f"/msg_u_{name}"

            localTmpFilePath = "./video_info_scrap.json"

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
            "pageSize": ONE_TIME_CMS_FETCH_COUNT,
            "isMsg": 1,
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

def getLinkType(msg: types.Message):
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

def getText(msg: types.Message):
    # 2- Vedio视频消息 3- Photo图片消息 4- Text文本消息 5- Audio音乐消息 6- File文件消息
    text = msg.text or msg.caption
    return text or ""

def getUpdateDict(msg: types.Message):
    res = {}
    if msg.video:
        res = {
            "duration": msg.video.duration,
            "fileSize": msg.video.file_size,
            "fileName": msg.video.file_name,
        }
    if msg.audio:
        res = {
            "duration": msg.audio.duration,
            "fileSize": msg.audio.file_size,
            "fileName": msg.audio.file_name,
        }
    if msg.document:
        res = {
            "fileSize": msg.document.file_size,
            "fileName": msg.document.file_name,
        }
    if msg.photo:
        res = {
            "fileSize": msg.photo.file_size,
        }
    return res

cms = CMSService()
pattern = f'^https://t\.me/([^/]+)/(?:([^/]+)/)?(\d+)$'

async def logicCode(bot_name: str, client: Client):
    # 获取到需要搜索关键字
    links = cms.getLinkToVerify()
    payload = []
    for link in links:
        matching = re.findall(pattern=pattern, string=link)
        print(matching)
        if matching:
            m_username, _, m_id = matching[0]
            print(f"Username {m_username} MID: {m_id}")
            try:
                msg = await client.get_messages(
                    chat_id=m_username,
                    message_ids=int(m_id),
                )
                if msg.empty:
                    continue
                linkType = getLinkType(msg=msg)
                text = getText(msg=msg)
                updateDict = getUpdateDict(msg=msg)
                info = {
                    "isValid": 1,
                    "url": link,
                    "source": 7,
                    "linkType": linkType,
                    "text": text,
                    "publishTime": msg.date.isoformat(),
                    "readNum": msg.views,
                    # "duration": msg.video.duration,
                    # "fileSize": 100,
                    # "fileName": ""
                }
                info.update(updateDict)
                payload.append(info)
            except Exception as e:
                print(e)
                payload.append({
                    "isValid": 0,
                    "url": link,
                })
        time.sleep(5)
        # time.sleep(ONE_TIME_CMS_FETCH_COUNT / (LINK_VERIFY_COOLDOWN* 1.5))
    return payload

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
            print(await client.get_me())
            payload = await logicCode(bot_name=bot_name, client=client)
            print(payload)
            save = cms.collectGroupInfo(payload)
            print(save)
            await client.stop()
    except Exception as e:
        print("Exception out")
        print(e)
    finally:
        TOKENS.insert(0, bot_session)

async def busineseRequirement2():
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
    clientObj = clients.pop()
    clients.insert(0, clientObj)
    # 下次切换账号时间
    change_time = datetime.now() + timedelta(seconds=change_client_period)
    # 循环执行
    while err_msg == "":
        try:
            # 获取待查询链接
            links = cms.getLinkToVerify()
            # 无待查询链接休眠10分钟
            if len(links) == 0:
                print(f"{datetime.now()},no data，sleep 10 minutes")
                time.sleep(600)
                continue

            # 循环处理链接
            payload = []
            start_time = datetime.now()
            for link in links:
                matching = re.findall(pattern=pattern, string=link)
                # print(matching)
                if matching:
                    m_username, _, m_id = matching[0]
                    # print(f"{datetime.now()},Username {m_username} MID: {m_id}")
                    try:
                        # 周期切换账号
                        if datetime.now() >= change_time:
                            clientObj = clients.pop()
                            clients.insert(0, clientObj)
                            change_time = datetime.now() + timedelta(seconds=change_client_period)
                        client = clientObj["client"]
                        token_name = clientObj["tokenName"]
                        # 获取信息
                        total_cnt += 1
                        msg = await client.get_messages(chat_id=m_username, message_ids=int(m_id))
                        # print(f"{datetime.now()},getmsg")
                        if msg.empty:
                            print(f"msg is empty,link:{link}")
                            continue

                        # 解析数据
                        linkType = getLinkType(msg=msg)
                        text = getText(msg=msg)
                        updateDict = getUpdateDict(msg=msg)
                        info = {
                            "isValid": 1,
                            "url": link,
                            "source": 7,
                            "linkType": linkType,
                            "text": text,
                            "publishTime": msg.date.isoformat(),
                            "readNum": msg.views,
                            # "duration": msg.video.duration,
                            # "fileSize": 100,
                            # "fileName": ""
                        }
                        info.update(updateDict)

                        # 待发送数据
                        payload.append(info)
                    except errors.FloodWait as e:
                        # print(f"{datetime.now()}, {token_name}, Waiting for {e.value} seconds.")
                        # await asyncio.sleep(e.value)
                        err_msg = f"{token_name}: Waiting for {e.value} seconds."
                        break
                    except Exception as e:
                        # err_msg = str(e)
                        # break
                        print(f"{datetime.now()}, tokenName:{token_name}, url:{link}, error: {e}")
                    finally:
                        # clients.insert(0, clientObj)
                        # num = round(4 / len(clients), 1)
                        # num = random.randrange(int(num * 0.6 * 100), int(num * 1.4 * 100)) / 100
                        time.sleep(per_link_sleep)
            # 发送数据
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
                bot_token=token["token"],
        ) as client:
            while err_msg == "":
                try:
                    links = cms.getLinkToVerify()
                    if len(links) == 0:
                        print(f"{datetime.now()},no data，sleep 10 minutes")
                        time.sleep(600)
                    payload = []
                    start_time = datetime.now()
                    success_cnt = 0
                    for link in links:
                        matching = re.findall(pattern=pattern, string=link)
                        # print(matching)
                        if matching:
                            m_username, _, m_id = matching[0]
                            # print(f"{datetime.now()},Username {m_username} MID: {m_id}")
                            try:
                                # print(await client.get_me())
                                total_cnt += 1
                                msg = await client.get_messages(
                                    chat_id=m_username,
                                    message_ids=int(m_id),
                                )
                                # print(f"{datetime.now()},getmsg")
                                if msg.empty:
                                    # time.sleep(600)
                                    continue
                                    # err_msg = f"msg is empty,link:{link}"
                                    # break
                                linkType = getLinkType(msg=msg)
                                text = getText(msg=msg)
                                updateDict = getUpdateDict(msg=msg)
                                info = {
                                    "isValid": 1,
                                    "url": link,
                                    "source": 7,
                                    "linkType": linkType,
                                    "text": text,
                                    "publishTime": msg.date.isoformat(),
                                    "readNum": msg.views,
                                    # "duration": msg.video.duration,
                                    # "fileSize": 100,
                                    # "fileName": ""
                                }
                                info.update(updateDict)
                                success_cnt += 1
                                payload.append(info)
                            # except errors.MessageIdInvalidError:
                            #     success_cnt += 1
                            #     payload.append({"isValid": 0, "url": link})
                            # except (errors.PeerIdInvalid, errors.UsernameNotOccupied, ValueError) as e:
                            #     success_cnt += 1
                            #     payload.append({"isValid": 0, "url": link})
                            except errors.FloodWait as e:
                                # print(f"Rate limit exceeded. Waiting for {e.x} seconds.")
                                # await asyncio.sleep(e.value)
                                user_name = (await client.get_me()).username
                                err_msg = f"{datetime.now()},userName:{user_name},error:Rate limit exceeded. Waiting for {e.value} seconds"
                                break
                            except Exception as e:
                                # if 'Username not found' in str(e):
                                #     success_cnt += 1
                                #     payload.append({"isValid": 0, "url": link})
                                #     continue
                                # err_msg = str(e)
                                # break
                                print(f"url:{link}, error: {e}")
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
    try:
        links = cms.getLinkToVerify()
        if len(links) == 0:
            print(f"{datetime.now()},no data，sleep 10 minutes")
            time.sleep(600)
        payload = []
        print(f"{datetime.now()} start")
        for link in links:
            matching = re.findall(pattern=pattern, string=link)
            # print(matching)
            if matching:
                m_username, _, m_id = matching[0]
                print(f"{datetime.now()},Username {m_username} MID: {m_id}")
                try:
                    bot_session = TOKENS.pop()
                    bot_name = bot_session["bot_name"]
                    print(f"{datetime.now()},startcleint")
                    async with Client(
                            name=bot_name,
                            api_id=20079525,
                            api_hash="3365973876c9cab387e0629d690fbe19",
                            bot_token=bot_session["token"],
                    ) as client:
                        # print(await client.get_me())
                        print(f"{datetime.now()},get_messages")
                        msg = await client.get_messages(
                            chat_id=m_username,
                            message_ids=int(m_id),
                        )
                        print(f"{datetime.now()},getmsg")
                        if msg.empty:
                            continue
                        linkType = getLinkType(msg=msg)
                        text = getText(msg=msg)
                        updateDict = getUpdateDict(msg=msg)
                        info = {
                            "isValid": 1,
                            "url": link,
                            "source": 7,
                            "linkType": linkType,
                            "text": text,
                            "publishTime": msg.date.isoformat(),
                            "readNum": msg.views,
                            # "duration": msg.video.duration,
                            # "fileSize": 100,
                            # "fileName": ""
                        }
                        info.update(updateDict)
                        payload.append(info)
                except Exception as e:
                    print(e)
                    # payload.append({
                    #     "isValid": 0,
                    #     "url": link,
                    # })
                finally:
                    TOKENS.insert(0, bot_session)
                # time.sleep(1)
        print(f"{datetime.now()} end,{payload}")
        cms.collectGroupInfo(payload)
    except Exception as e:
        print("Exception out")
        print(e)

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

async def main():
    # run_scheduler()

    # try:
    #     while True:
    #         asyncio.run(busineseRequirement2())
    # except Exception as e:
    #     print(f"busineseRequirement2 error: {e}")

    try:
        await busineseRequirement2()
    except Exception as e:
        print(f"{datetime.now()},busineseRequirement2 error: {e}")
    #
    # try:
    #     tasks = []
    #     for token in TOKENS:
    #         tasks.append(busineseRequirement3(token))
    #     await asyncio.gather(*tasks)
    # except Exception as e:
    #     print(f"busineseRequirement3 error: {e}")

    # try:
    #     api_id = 20079525
    #     api_hash = "3365973876c9cab387e0629d690fbe19"
    #     for token in TOKENS:
    #         client = Client(name=token["bot_name"], api_id=api_id, api_hash=api_hash, bot_token=token["token"])
    #         client.start()
    #         clients.append(client)
    #     await busineseRequirement4()
    # except Exception as e:
    #     print(f"busineseRequirement4 error: {e}")

if __name__ == "__main__":
    # bot.run(main())
    print("RUNNING")
    asyncio.run(main())

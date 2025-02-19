from pyrogram import Client, filters, errors, idle
from cryptography.fernet import Fernet
from urllib.parse import urlsplit
from threading import Thread
from datetime import datetime, timedelta
from queue import Queue
import portalocker
import os
import schedule
import json
import time
import sys
import copy
import re
import requests
import hashlib

appInfo = {}
message_queue = Queue()
CHAT_LINK = []
message_info = {"username": "111"}
get_last_channel_inx = None
global_cipherSuite = None
fileName = None
global_error_dir = None
global_snum_dir = None
send_nums = {"username": "0"}


def main():
    global message_info
    global global_cipherSuite
    global appInfo
    global global_error_dir
    global fileName
    global global_snum_dir
    global send_nums
    try:
        if len(sys.argv) > 1:
            current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            collection_dict = json.loads(sys.argv[1])
            session = collection_dict["session"]
            # session = "BAFsFqEAYbGqqeEf4gT87SoSk22mnTqEv3yYvhyK2sGujzGS8TEW0hVd4p22tJ84ZVxMGsYIQsFCrrCPmLw-Dni0PEND1ax3G7TgUxcXFlegDNi3XnCHw4vNgQ6s9_RAY2ni91Kr7JVMiPiZpnvUWeJvwCTVkJWPo_MTYf1_sg2wKh1Ib_mioL6zd-6DVl8RQLWEG4NikzhQ3sYqeSqk0RCt-tKhhlCdXZsAhXqs-fgL68uuLh2kMgDwD0IGXOAmKg9wPx_vsfzCjQj03c49MRvjA0CIIGF1dEZ8JQvI10fcYHuXPAGZPYXt3_tNaPOAY9y3GmKihU3jaIk-pkwapO1YTVQpCwAAAAF1GEUhAA"
            appId = collection_dict["appId"]
            appName = collection_dict["appName"]
            appHash = collection_dict["appHash"]
            # appId="23860897"
            fileRootPath = collection_dict["fileRootPath"]
            # fileRootPath="C:/py_file/file_res/"
            fileName = collection_dict["fileName"]
            # fileName="ss"
            errorPath = collection_dict["errorPath"]
            global_error_dir = errorPath + appId + '-error.txt'

            # global_error_dir = "C:/py_file/error_res/"
            key_s = collection_dict["key"].encode('utf-8')
            global_cipherSuite = Fernet(key_s)
            print(f"---------------------------------appId:{appId}--------------------------")
            appInfo["appId"] = appId
            domain_names = ["t.me"]
            global_join_dir = fileRootPath + appId + '-join.txt'
            if os.path.exists(global_join_dir):
                with open(global_join_dir, 'r') as f_join:
                    join_infos = f_join.readline()
                    if join_infos:
                        CHAT_LINK = json.loads(join_infos)
            # 获取轮询频道的消息记录对象
            global_msg_dir = fileRootPath + 'messageInfo.txt'
            if os.path.exists(global_msg_dir):
                with open(global_msg_dir, 'r') as m_info_r:
                    msg_info = m_info_r.readline()
                    if msg_info:
                        message_info = json.loads(msg_info)
            # 记录发送给cms的消息数
            global_snum_dir = fileRootPath + appId + '-sennum.txt'
            if os.path.exists(global_snum_dir):
                with open(global_snum_dir, 'r') as snum_join:
                    snum_info = snum_join.readline()
                    if snum_info:
                        send_nums = json.loads(snum_info)

            get_last_channel_inx = 0
            # 获取渠道轮询下标
            global_channel_dir = fileRootPath + 'channelInx.txt'
            if os.path.exists(global_channel_dir):
                with open(global_channel_dir, 'r') as m_c_inx_r:
                    channel_inx_str = m_c_inx_r.readline()
                    if channel_inx_str:
                        get_last_channel_inx = int(channel_inx_str)
            channel_list_len = len(CHAT_LINK)
            if get_last_channel_inx >= channel_list_len:
                get_last_channel_inx = 0
            channel_name = CHAT_LINK[get_last_channel_inx]
            print(channel_name)
            group_num = send_nums.get(channel_name)
            if group_num:
                group_num = int(group_num)
            else:
                group_num = 0
            # 找到对应的channel消息信息
            get_last_channel_inx += 1
            app = Client(appName, api_id=appId, api_hash=appHash, session_string=session)
            with app:
                try:
                    history_msgs = app.get_chat_history(channel_name, limit=300)
                    # # 遍历到上次锚点id
                    list_inx = 0
                    last_id = 0
                    formatted_str = None
                    saveInfos = []
                    channel_lastmsg_id = message_info.get(channel_name)
                    for message in history_msgs:
                        last_id = message.id
                        if channel_lastmsg_id and last_id == channel_lastmsg_id:
                            print(
                                f"---------------------------------There is no new news channel_name:{channel_name}--------------------------")
                            break
                        if list_inx == 0:
                            message_info[channel_name] = last_id

                        try:
                            url_list = []
                            msg_txt = ""
                            chat = message.chat
                            if chat.username is None:
                                continue
                            # chat_url="https://t.me/"+chat.username
                            chat_url = None
                            if hasattr(message, 'text') and message.text:
                                msg_txt = message.text
                                if msg_txt:
                                    url_list = find_tme_urls(msg_txt)
                            if hasattr(message, 'caption') and message.caption:
                                msg_txt = message.caption
                            is_sys_msg_flag = False
                            if hasattr(message, 'service'):
                                is_sys_msg_flag = True

                            if hasattr(message, 'caption_entities') and message.caption_entities:
                                for entity_obj in message.caption_entities:
                                    entity_type = str(entity_obj.type)
                                    if entity_type == "MessageEntityType.TEXT_LINK":
                                        addr_link = str(entity_obj.url)
                                        parts = urlsplit(addr_link)
                                        paths = parts.path.split('/')
                                        if len(paths) == 2 and addr_link not in url_list:
                                            url_list.append(addr_link)
                            if hasattr(message, 'entities') and message.entities:
                                for entity_obj in message.entities:
                                    entity_type = str(entity_obj.type)
                                    if entity_type == "MessageEntityType.TEXT_LINK":
                                        addr_link = str(entity_obj.url)
                                        parts = urlsplit(addr_link)
                                        paths = parts.path.split('/')
                                        if len(paths) == 2 and addr_link not in url_list:
                                            url_list.append(addr_link)

                            message_type = str(message.media)
                            is_msg_ok = False
                            if is_sys_msg_flag or message_type == "MessageMediaType.PHOTO" or message_type == "MessageMediaType.VIDEO" or message_type == "MessageMediaType.AUDIO" or message_type == "MessageMediaType.DOCUMENT":
                                is_msg_ok = True

                            if msg_txt or url_list or is_msg_ok:

                                chat_url = "https://t.me/" + chat.username
                                channel_info = structure_chat_info(
                                    chat, message, 1, chat_url, url_list, msg_txt, appId, False)
                                if channel_info:
                                    if list_inx == 0:
                                        print(channel_info)
                                    saveInfos.append(channel_info)

                        except Exception as e1:
                            e_str = str(e1)
                            print("get_chat_history foreach error: " + e_str)

                        list_inx += 1
                    if saveInfos:
                        send_infos = []
                        set_len = len(saveInfos)
                        len_inx = 0
                        print(set_len)
                        group_num += set_len
                        send_nums[channel_name] = group_num
                        for channelItem in saveInfos:
                            if len(send_infos) >= 20:
                                # a5b684db7f9d746b18f60bbe60781a74-8b898802e7cba134.elb.ap-southeast-1.amazonaws.com:8800  生产
                                # a901c69ff2d4c4b9bb678f3ebc6ea4c1-a52e4692b9f44640.elb.ap-southeast-1.amazonaws.com:8800  测试
                                url = 'http://a276b8d3ca3a14befa1dc6335eaa47ea-f83cb44aa303c283.elb.ap-southeast-1.amazonaws.com:8800/cms-service/cms/content/collect'
                                # url="http://a901c69ff2d4c4b9bb678f3ebc6ea4c1-a52e4692b9f44640.elb.ap-southeast-1.amazonaws.com:8800/cms/content/collect"
                                headers = sendHeaderGenerate(send_infos)
                                response = requests.post(url, json=send_infos, headers=headers)
                                send_infos = []
                                send_infos.append(channelItem)
                                print(response.text)
                            else:
                                if channelItem:
                                    send_infos.append(channelItem)
                            len_inx += 1
                            if len_inx == set_len and send_infos:
                                print("send end")
                                url = 'http://a276b8d3ca3a14befa1dc6335eaa47ea-f83cb44aa303c283.elb.ap-southeast-1.amazonaws.com:8800/cms-service/cms/content/collect'
                                headers = sendHeaderGenerate(send_infos)
                                response = requests.post(url, json=send_infos, headers=headers)
                                send_infos = []
                                print(response.text)
                        with open(global_snum_dir, 'w') as snum_join_w:
                            send_num_str = json.dumps(send_nums)
                            snum_join_w.write(send_num_str)
                    print(f"---------------------------------channelListen_run end:{appId}--------------------------")
                except Exception as e1:
                    e1_str = str(e1)
                    print("get_chat_history error: " + e1_str)

                with open(global_msg_dir, 'w') as m_info_w:
                    message_info_str = json.dumps(message_info)
                    m_info_w.write(message_info_str)

                with open(global_channel_dir, 'w') as m_c_inx_w:
                    m_c_inx_w.write(str(get_last_channel_inx))

    except Exception as e:
        e_str = str(e)
        print("channelListen_run error: " + e_str)
        appInfo["sendNum"] = "0"
        appInfo["errorType"] = "Account"
        appInfo["isClose"] = False
        writeError(appInfo, e, True, fileName, global_cipherSuite, global_error_dir)


def writeError(appInfo, emsg, isError, fileName, global_cipherSuite, global_error_dir):
    print("writeError")
    if appInfo:
        error_msg = ""
        if isError:
            error_msg = ' '.join(map(str, emsg.args)).replace('"', '').replace("'", "")
        else:
            error_msg = emsg
        current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        error_msg = "sendNum " + appInfo["sendNum"] + " error_time:" + current_time + " error_msg:" + error_msg
        appInfo["errorMsg"] = error_msg
        appInfo["fileName"] = fileName
        msg_str = json.dumps(appInfo)
        encrypted_data = global_cipherSuite.encrypt(msg_str.encode('utf-8'))
        local_f = open(global_error_dir, 'wb')
        local_f.write(encrypted_data)
        local_f.flush()
        local_f.close()


def find_tme_urls(text):
    # 正则表达式模式，匹配以http开头，紧跟"t.me"的字符串
    pattern = r'https://t.me/[^\s]+'
    # 使用findall方法找到所有匹配的URL
    urls = re.findall(pattern, text)
    return urls


def structure_chat_info(chat, message, chat_status, chat_url, url_list, msg_txt, appId, is_invalid):
    channel_info = {}
    try:

        # if chat.__class__.__name__ == "Chat":
        chat_type = str(chat.type)
        description = None
        formatted_str = None
        members_count = None
        if chat.description:
            description = chat.description
        if chat.members_count:
            members_count = chat.members_count
        msg_obj = None
        sys_msg_type = None
        if message:
            date_last = message.date
            formatted_str = date_last.strftime('%Y-%m-%d %H:%M:%S')
            chat_url = "https://t.me/" + chat.username
            link_type = 4
            read_num = None
            file_duration = None
            file_size = None
            file_name = None
            msg_url = None
            if hasattr(message, 'id'):
                msg_url = chat_url + "/" + str(message.id)
            if hasattr(message, 'service'):
                if message.service:
                    sys_msg_type = str(message.service)
                    link_type = None
            if hasattr(message, 'views') and message.views:
                read_num = message.views
            # 2- Vedio视频消息 3- Photo图片消息 4- Text文本消息 5- Audio音乐消息 6- File文件消息
            if hasattr(message, 'media') and message.media:
                message_type = str(message.media)
                if message_type == "MessageMediaType.PHOTO":
                    link_type = 3
                    if hasattr(message, 'photo') and message.photo:
                        photo_obj = message.photo
                        file_size = photo_obj.file_size
                if message_type == "MessageMediaType.VIDEO":
                    link_type = 2
                    if hasattr(message, 'video') and message.video:
                        video_obj = message.video
                        file_duration = video_obj.duration
                        file_size = video_obj.file_size
                        file_name = video_obj.file_name
                if message_type == "MessageMediaType.AUDIO":
                    link_type = 5
                    if hasattr(message, 'audio') and message.audio:
                        audio_obj = message.audio
                        file_duration = audio_obj.duration
                        file_size = audio_obj.file_size
                        file_name = audio_obj.file_name
                if message_type == "MessageMediaType.DOCUMENT":
                    link_type = 6
                    if hasattr(message, 'document') and message.document:
                        document_obj = message.document
                        file_size = document_obj.file_size
                        file_name = document_obj.file_name

            msg_obj = {
                "urls": url_list,
                "url": msg_url,
                "publishTime": formatted_str,
                "sysMsgType": sys_msg_type,
                "text": msg_txt,
                "readNum": read_num,
                "duration": file_duration,
                "fileSize": file_size,
                "fileName": file_name,
                "linkType": link_type
            }
        channel_info = {
            "url": chat_url,
            "linkType": 1 if chat_type == "ChatType.CHANNEL" else 0,
            "status": chat_status,
            "telegramId": chat.id,
            "source": 0,
            "extendJson": {
                "name": chat.title,
                "description": description,
                "userNum": members_count,
                "userName": chat.username if chat.username else None,
                "isIOS": 1 if chat.is_restricted else 0,
                "isScam": 1 if chat.is_scam else 0,
                "isFake": 1 if chat.is_fake else 0,
                "createdTime": None,
                "owner": None,
                "admins": [],
                "longAppId": appId
            },
            "msg": msg_obj
        }
        return channel_info
    except Exception as e:
        e_str = str(e)
        print("channel_listen_structure_chat_info error:" + e_str)
        return False


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
    SERVER_APP_KEY = "1"
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


if __name__ == "__main__":
    main()
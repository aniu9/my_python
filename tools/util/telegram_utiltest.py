import time

from pyrogram import Client
from pyrogram.enums import ParseMode

# 166 cobb923 fq_collect_bot
_api_id = '26534384'
_api_hash = '8c32b586b4c83b04b9ab7d50c6464907'
_bot_token = '6854483464:AAEAIZoirC7URAVajssHby4ZHzox9EduPIM'
_bot = Client('mybot1', _api_id, _api_hash, bot_token=_bot_token)
_client = Client('myapp1', _api_id, _api_hash)
# cjaniu
_user_id = 5380352615


# def get_bot():
#     bot = Client('myapp', "", "", bot_token=_bot_token)
#     return bot
#
# def get_client():
#     client = Client('myapp', _api_id, _api_hash)
#     return client

def get_bot_me():
    # https://api.telegram.org/bot7480786871:AAGocBV8ne1jli8HXzlC931rIj1CunpUhgg/getMe
    with _bot:
        me = _bot.get_me()
    print(f'id:{me.id}，userName:{me.username}')


def get_client_me():
    # https://api.telegram.org/bot7480786871:AAGocBV8ne1jli8HXzlC931rIj1CunpUhgg/getMe
    with _client:
        me = _client.get_me()
    print(f'id:{me.id}，userName:{me.username}')


def bot_send_msg(char_id, msg):
    with _bot:
        _bot.send_message(char_id, msg, parse_mode=ParseMode.HTML)


def get_chat_info(chat_id):
    with _bot:
        chat = _bot.get_chat(chat_id)
        print(f'id:{chat.id}，userName:{chat.username}')
        # print(f'id:{chat.id}，userName:{chat.username},userNum:{chat.members_count},name:{chat.title},description:{chat.description},linkType:{chat.type.value}')


def get_chat_members(chat_id):
    with _bot:
        members = _bot.get_chat_members(chat_id)
        for member in members:
            print(f'id:{member.user.id}，userName:{member.user.username}，memberType:{member.status}')
    # get_chat_members_count
    # ChatMemberStatus.MEMBER
    # ChatMemberStatus.OWNER
    # ChatMemberStatus.ADMINISTRATOR


def get_group_msgs(group_id, min_id):
    with _client:
        offset_id = 0
        while 1 == 1:
            messages = _client.get_chat_history(group_id, 2, offset_id=offset_id)
            for msg in messages:
                offset_id = msg.id
                if msg.id <= min_id:
                    return
                print(f"{msg.id},{msg.date}")


def forward():
    # 定义转发规则
    forward_rules = {
        '5380352615': 'mygroup423',  # 从源聊天转发到目标聊天
        # 可以添加更多规则
    }

    @_bot.on_message()
    async def forward_handler(client, message):
        # 获取消息的协议号
        protocol_number = message.from_user.id

        # 获取源聊天 ID
        source_chat_id = message.chat.id

        # 获取目标聊天 ID
        target_chat_id = forward_rules[str(source_chat_id)]

        # 构造转发的消息
        forwarded_message = f"协议号 {protocol_number}:\n\n{message.text}"

        # 转发消息到目标聊天
        await client.send_message('mygroup423', forwarded_message)

        print(f"消息已从 {source_chat_id} 转发到 {target_chat_id}")


def bot_run():
    try:
        print('可以透过 ctrl+c中断服务')
        _bot.run(forward())
    except KeyboardInterrupt:
        print('已经透过 ctrl+c 强制中断了')
    except:
        print('其他例外异常')


if __name__ == "__main__":
    count = 0
    urls=['driftcast',
          'HavalRussiaOfficial',
          'islamdag',
          'termux_tyt',
          'OSINT_DataHUB',
          'feets_soles',
          'ne_investor',
          'rus_buzines',
          'germany_muzyka',
          'glavdoroga',
          'ptencoff',
          'zonaosoboho',
          'lawandtattoo1',
          'amigokononchat',
          'stalcraft_old_comments',
          'fishzakup_perm',
          'emojic',
          'marketplace_wbchat',
          'o1eb1eb2',
          'egyptologysolkin',
          'people360_polyglot',
          'offsider_chat',
          'sportvestnik24',
          'vanesspubg',
          'liga_apologetov',
          'chp_chat_tut',
          'ai4telegram',
          'avitolagi',
          'vdavesna_2021',
          'culinaryabuse',
          'termuxml',
          'kostina_ru',
          'matchtv_highlights',
          'Mireaio',
          'makvayLive',
          'memy_video_smeh',
          'netmonet_co',
          'portalcoin_community',
          'poisk_rabot_kyiv',
          'msk_live',
          'velyastand',
          'secretkonditera',
          'stroitel_ahmed',
          'secretni',
          'rkn_info',
          'pstmarketing',
          'vsenapyat',
          'fanatmmaa',
          'arkein3',
          'freeastrologyvera',
          'casino_bounty',
          'frilanser_vacansii',
          'poyasnuzaufc',
          'whrmediachat',
          'taroworlds',
          'kosmo_woman22',
          'baliktoletit',
          'antalyaspeakingclub07',
          'lekar_lnr',
          'maxkorzhwarszawa',
          'vyzanie6',
          'wDuRCxzYVzM5Nzcy',
          'puppyangel_911',
          'official_xochat',
          'InvestBizSpb',
          'psyhoworld2024',
          'igeliztz',
          'musixcx13',
          'rozygryshi_tg',
          'vyshivka_tg',
          'zhiznvmoichrukah',
          'pngmali',
          'inongame',
          'tons_base',
          'nesovetchik',
          'artisnewsexy',
          'otpbanknews',
          'prostyazikom',
          'gorbushkin_ru',
          'byjsnm',
          'popmma_tg',
          'royale_pabg_pubg',
          'shef_craba_nizniy_novgorod',
          'Hand_made_Russ',
          'CasinoLudomaniaChatik',
          'mining_irkutskk',
          'otcship',
          'chatmassagistov',
          'poyasnyalichmma',
          'PeintoOfficial',
          'Tyumenprotivprinuzhdeniya',
          'womenhealthdmitrov',
          'musplatinum',
          'sthworld',
          'mamatweenss',
          'whooshbike',
          'topor_novosti_voina',
          'uristantondolgih']
    while (count<len(urls)):
        url=urls[count];
        count = count + 1
        print(count)
        try:
            get_chat_info(url)
        except Exception as e:
            print(e)
        time.sleep(3)
        # get_chat_members(url)
        # time.sleep(3)
    # supergroup channel

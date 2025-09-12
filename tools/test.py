import asyncio
from pyrogram import Client
from pyrogram.types import ChatPrivileges

API_ID = '26534384'
API_HASH = '8c32b586b4c83b04b9ab7d50c6464907'

TOKEN_NAME = 'fq_collect_bot'
BOT_TOKEN = '7931792484:AAEX1QM7483FqCpcRvwFBEdbr2_Hb6uWp5s'

async def add(chat_id, target_id):
    async with Client('client_session', API_ID, API_HASH, bot_token=BOT_TOKEN) as client:
        await client.add_chat_members(chat_id, target_id)
    print(1)

async def promote(chat_id, target_id):
    # await add(chat_id, target_id)
    privileges = ChatPrivileges(
        can_manage_chat=True,  # 基本聊天管理（必需）
        can_delete_messages=True,  # 删除消息
        can_manage_video_chats=True,  # 管理视频聊天（会议）
        can_restrict_members=True,  # 禁言/封禁成员
        can_promote_members=True,  # 提升其他成员（谨慎授予！）
        can_change_info=True,  # 更改聊天信息
        can_post_messages=True,  # 在频道发帖（如果是频道）
        can_edit_messages=True,  # 编辑消息（如果是频道）
        can_invite_users=True,  # 邀请用户
        can_pin_messages=True,  # 置顶消息
        is_anonymous=False  # 匿名操作（仅限超级群组）
    )

    async with Client('client_session', API_ID, API_HASH, bot_token=BOT_TOKEN) as client:
        await client.promote_chat_member(
            chat_id=chat_id,
            user_id=target_id,
            privileges=privileges
        )

if __name__ == "__main__":
    asyncio.run(promote('cjtestchanel', 'aniu923'))

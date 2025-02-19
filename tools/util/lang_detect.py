import os

import langid
from langdetect import LangDetectException

_cur_dir = os.path.dirname(__file__)
_root_dir = os.path.dirname(_cur_dir)


def is_en_char(c):
    return (c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z')


def is_cn_char(c):
    cn_min = ord('\u4e00')
    cn_max = ord('\u9fa5')
    char_int = ord(c)
    if char_int < cn_min or char_int > cn_max:
        return False
    else:
        return True


def detect_lang(text):
    total_cnt = len(text)
    if total_cnt < 10:
        return 'unknown'

    try:
        lang, _ = langid.classify(text)
        lang = 'zh-cn' if lang == 'zh' else lang
        return lang
        # return detect(text)
    except LangDetectException:
        return "unknown"
    #
    # en_cnt = 0
    # cn_cnt = 0
    # for char in text:
    #     if is_cn_char(char):
    #         cn_cnt += 1
    #     elif is_en_char(char):
    #         en_cnt += 1
    #
    # if cn_cnt > 5:
    #     return 'zh-cn'
    # elif en_cnt / total_cnt > 0.6:
    #     return 'en'
    #
    # return 'unknown'  # 默认返回值，表示无法确定语言

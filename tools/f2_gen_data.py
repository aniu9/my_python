import asyncio
import copy
import uuid

import pandas as pd
import random

import requests
from faker import Faker
from datetime import datetime, timedelta
import json

def fake_cms_excel():
    random.seed(42)

    LANGS = ['zh-cn', 'zh-tw', 'en']
    CONTENT_TYPES = ['job', 'rental', 'news']
    REGIONS = ['CN', 'US']
    STATUS = ['published', 'draft']

    # 自定义一些常用城市，用于 location 字段
    cities = [
        "柬埔寨金边", "上海", "北京", "纽约", "洛杉矶",
        "东京", "首尔", "新加坡", "悉尼", "伦敦"
    ]

    def random_publish_time():
        return datetime.now() - timedelta(days=random.randint(0, 30))

    fake_en = Faker()
    fake_cn = Faker("zh_CN")
    fake_tw = Faker("zh_TW")

    def get_fake(lang):
        if lang == 'zh-cn':
            return fake_cn  # 中文简体
        elif lang == 'zh-tw':
            return fake_tw  # 中文繁体
        else:
            return fake_en

    def generate_fake_data():
        """
        使用 Faker 库生成假数据示例。
        """
        fake = Faker("zh_CN")  # 设置中文环境
        data = {
            "name": fake.name(),
            "address": fake.address(),
            "phone": fake.phone_number(),
            "email": fake.email(),
            "date": fake.date(),
            "text": fake.text(max_nb_chars=100),
            "title": fake.sentence(nb_words=6),
            "summary": fake.text(max_nb_chars=80),
            "body": fake.paragraph(nb_sentences=5),
            "image_url": fake.image_url(),
            "word": fake.word(),
        }
        return data

    rows_content = []
    rows_lang = []
    rows_meta = []
    rows_media = []
    rows_c2c = []
    cat_sample = [4, 5, 6, 7, 8, 9, 10]  # 示例子类别ID

    for cid in range(1, 51):
        ctype = random.choice(CONTENT_TYPES)
        status = random.choice(STATUS)
        pub = random_publish_time()
        cr = pub - timedelta(days=random.randint(1, 5))
        up = cr + timedelta(days=random.randint(0, 5))
        rows_content.append([
            cid, ctype, status, random.choice(REGIONS),
            random.randint(0, 1), pub, cr, up
        ])

        for lang in LANGS:
            fake = get_fake(lang)
            title = fake.sentence(nb_words=6)
            summary = fake.text(max_nb_chars=80)
            body = fake.paragraph(nb_sentences=5)
            rows_lang.append([
                len(rows_lang) + 1, cid, lang, title, body,
                summary, title + " | " + lang, title[:70], cr, up
            ])

            meta = {
                "location": random.choice(cities),
                "job_description": fake.sentence(nb_words=15)
            }

            meta_json_str = json.dumps(meta, ensure_ascii=False, indent=2)
            rows_meta.append([
                len(rows_meta) + 1, cid, lang, meta_json_str, cr, up
            ])

        for _ in range(random.randint(1, 2)):
            rows_media.append([
                len(rows_media) + 1, cid, random.choice(['image', 'video']),
                fake_en.image_url(), random.randint(1, 10), fake_en.word(), cr, up
            ])

        for cat in random.sample(cat_sample, k=random.randint(1, 2)):
            rows_c2c.append([
                len(rows_c2c) + 1, cid, cat, cr, up
            ])

    df_content = pd.DataFrame(rows_content, columns=[
        'id', 'content_type', 'status', 'region_code',
        'is_featured', 'publish_time', 'create_time', 'update_time'
    ])

    df_content_lang = pd.DataFrame(rows_lang, columns=[
        'id', 'content_id', 'lang_code', 'title', 'body',
        'summary', 'seo_title', 'seo_description',
        'create_time', 'update_time'
    ])

    df_meta = pd.DataFrame(rows_meta, columns=[
        'id', 'content_id', 'lang_code', 'meta',
        'create_time', 'update_time'
    ])

    df_media = pd.DataFrame(rows_media, columns=[
        'id', 'content_id', 'media_type', 'url',
        'sort', 'alt_text', 'create_time', 'update_time'
    ])

    df_category2content = pd.DataFrame(rows_c2c, columns=[
        'id', 'content_id', 'category_id', 'create_time', 'update_time'
    ])

    with pd.ExcelWriter('模拟内容_openpyxl.xlsx', engine='openpyxl') as writer:
        df_content.to_excel(writer, sheet_name='content', index=False)
        df_content_lang.to_excel(writer, sheet_name='content_lang', index=False)
        df_meta.to_excel(writer, sheet_name='meta', index=False)
        df_media.to_excel(writer, sheet_name='media', index=False)
        df_category2content.to_excel(writer, sheet_name='category2content', index=False)

    print("✅")

def fake_cms_json():
    random.seed(42)

    LANGS = ['zh-cn', 'zh-tw', 'en']
    CONTENT_TYPES = ['job', 'rental', 'news']
    REGIONS = ['CN', 'US']
    STATUS = ['published', 'draft']

    # 自定义一些常用城市，用于 location 字段
    cities = [
        "柬埔寨金边", "上海", "北京", "纽约", "洛杉矶",
        "东京", "首尔", "新加坡", "悉尼", "伦敦"
    ]

    def random_publish_time():
        return datetime.now() - timedelta(days=random.randint(0, 30))

    fake_en = Faker()
    fake_cn = Faker("zh_CN")
    fake_tw = Faker("zh_TW")

    def get_fake(lang):
        if lang == 'zh-cn':
            return fake_cn  # 中文简体
        elif lang == 'zh-tw':
            return fake_tw  # 中文繁体
        else:
            return fake_en

    arr = []
    for cid in range(1, 51):
        arr_media = []
        for _ in range(random.randint(1, 2)):
            arr_media.append({
                "mediaType": random.choice(['image', 'video']),
                "url": fake_en.image_url(),
                "sort": random.randint(1, 10),
                "altText": fake_en.word(),
            })
        content = {
            "platType": random.choice(CONTENT_TYPES),
            "code": uuid.uuid4().hex,
            "status": 1,
            "regionCode": random.choice(REGIONS),
            "isFeatured": random.randint(0, 1),
            "publishTime": (random_publish_time() - timedelta(days=random.randint(1, 5))).isoformat(),
            "medias": arr_media
        }

        langs = []
        for lang in LANGS:
            fake = get_fake(lang)
            meta = {
                "location": random.choice(cities),
                "job_description": fake.sentence(nb_words=15)
            }
            lang = {
                "langCode": lang,
                "title": fake.sentence(nb_words=6),
                "body": fake.paragraph(nb_sentences=5),
                "summary": fake.text(max_nb_chars=80),
                "seoTitle": "",
                "seoDescription": "",
                "meta": meta
            }
            langs.append(lang)
        content["langs"] = langs
        content["categoryCodes"] = ["testoumei"]
        arr.append(content)

    return arr

def req_api(api, data):
    # url = "http://localhost:8092/" + api.strip("/")
    # data = json.dumps(data)
    headers = {
        'Content-Type': 'application/json'
    }
    try:
        response = requests.post(api, headers=headers, json=data, timeout=30)
        if response.status_code != 200:
            print({"error": f"Status code: {response.status_code}"})
            return []
        result = json.loads(response.content)
        if result["code"] != 200:
            print(f"error: {result}")
            return []
        return result
    except Exception as ex:
        print(ex)

async def cms_import_fake():
    arr = fake_cms_json()
    url = "http://a6ac7438791f7462895472cade21e351-8e8ffc566269d199.elb.ap-southeast-1.amazonaws.com:8800/cms/content/importContents"
    # url = "http://localhost:8092/cms/content/importContents"
    result = req_api(url, arr)
    print(result)

if __name__ == "__main__":
    # fake_cms_json()
    asyncio.run(cms_import_fake())

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import asyncio
import json
import requests

from tools.test import sendHeaderGenerate

# 目标 API 地址（请替换为你的实际 URL）
url = "http://a276b8d3ca3a14befa1dc6335eaa47ea-f83cb44aa303c283.elb.ap-southeast-1.amazonaws.com:8800/botmng-service/cms/filter/importFilter"


# 要提交的分类及其各语言翻译
categories = [
  {
    "id": "technology",
    "translations": {
      "zh-cn": "科技",
      "zh-tw": "科技",
      "en": "Technology",
      "km": "បច្ចេកវិទ្យា",
      "th": "เทคโนโลยี",
      "ms": "Teknologi"
    }
  },
  {
    "id": "business",
    "translations": {
      "zh-cn": "财经",
      "zh-tw": "財經",
      "en": "Business",
      "km": "អាជីវកម្ម",
      "th": "ธุรกิจ",
      "ms": "Perdagangan"
    }
  },
  {
    "id": "entertainment",
    "translations": {
      "zh-cn": "娱乐",
      "zh-tw": "娛樂",
      "en": "Entertainment",
      "km": "កម្សាន្ត",
      "th": "บันเทิง",
      "ms": "Hiburan"
    }
  },
  {
    "id": "sports",
    "translations": {
      "zh-cn": "体育",
      "zh-tw": "體育",
      "en": "Sports",
      "km": "កីឡា",
      "th": "กีฬา",
      "ms": "Sukan"
    }
  },
  {
    "id": "health",
    "translations": {
      "zh-cn": "健康",
      "zh-tw": "健康",
      "en": "Health",
      "km": "សុខភាព",
      "th": "สุขภาพ",
      "ms": "Kesihatan"
    }
  },
  {
    "id": "lifestyle",
    "translations": {
          "zh-cn": "生活",
      "zh-tw": "生活",
      "en": "Lifestyle",
      "km": "ជីវិត",
      "th": "ไลฟ์สไตล์",
      "ms": "Gaya hidup"
    }
  },
  {
    "id": "education",
    "translations": {
      "zh-cn": "教育",
      "zh-tw": "教育",
      "en": "Education",
      "km": "ការអប់រំ",
      "th": "การศึกษา",
      "ms": "Pendidikan"
    }
  },
  {
    "id": "travel",
    "translations": {
      "zh-cn": "旅游",
      "zh-tw": "旅遊",
      "en": "Travel",
      "km": "ទេសចរណ៍",
      "th": "การท่องเที่ยว",
      "ms": "Pelancongan"
    }
  },
  {
    "id": "science",
    "translations": {
      "zh-cn": "科学",
      "zh-tw": "科學",
      "en": "Science",
      "km": "វិទ្យាសាស្រ្ត",
      "th": "วิทยาศาสตร์",
      "ms": "Sains"
    }
  },
  {
    "id": "world",
    "translations": {
      "zh-cn": "国际",
      "zh-tw": "國際",
      "en": "World",
      "km": "ពិភពលោក",
      "th": "โลก",
      "ms": "Dunia"
    }
  }
]
def main():
    # 自动收集所有语言代码（不暴露语言列表）
    languages = sorted({lang for cat in categories for lang in cat["translations"].keys()})

    payload = []
    global_sort = 1

    for lang in languages:
        values = []
        inner_sort = 1
        for cat in categories:
            trans = cat["translations"].get(lang, cat["id"])
            values.append({
                "fieldValue": cat["id"],
                "valueText": trans,
                "sort": inner_sort,
                "status": 1,
                "parentValue": ""  # 根据需求调整
            })
            inner_sort += 1

        item = {
            "platType": "news",
            "langCode": lang,
            "sort": global_sort,
            "parentCode": "",
            "code": f"{lang}",
            "fieldName": "categoryCode",
            "displayName": "categoryCode",
            "fieldType": "string",
            "operator": "in",
            "status": 1,
            "values": values
        }

        payload.append(item)
        global_sort += 1

    payload_json = json.dumps(payload, ensure_ascii=False)
    print(payload_json)
    # payload_json=[]
    headers = sendHeaderGenerate(payload)
    print(headers)
    response = requests.post(url, headers=headers, json=payload)
    print("Response status:", response.status_code)
    print("Response body:", response.text)

# async def request():
#     url = 'http://a276b8d3ca3a14befa1dc6335eaa47ea-f83cb44aa303c283.elb.ap-southeast-1.amazonaws.com:8800/botmng-service/cms/filter/getFilters'
#     # url = 'http://localhost:8083/botmng-service/cms/filter/getFilters'
#     params={
#         "platType": "feibo",
#         "langCode": "zh-cn"
#     }
#     headers = sendHeaderGenerate(params)
#     response = requests.post(url, json=params, headers=headers)
#     print(response.text)
#
# if __name__ == "__main__":
#     asyncio.run(request())

if __name__ == "__main__":
    url = "http://localhost:8092/cms/content/importContents"
    param=[{'platType': 'news', 'status': 1, 'regionCode': '', 'isFeatured': 1, 'publishTime': '2025-05-03T23:08:23.550035+00:00', 'code': '34d93ce4549d42718691c66529e2e820', 'langs': [{'langCode': 'zh-cn', 'title': '以色列作为英国和盟国的加沙市轰炸加沙市要求采取行动，以防止“展开饥荒”', 'body': '该地区的哈马斯民防机构说，加沙市受到强烈的空袭，以色列部队准备发动行动来接管这座城市。 发言人马哈茂', 'summary': '这些国家要求“立即，永久和具体的步骤”，以促进向加沙的援助。', 'categoryCodes': 'zh-cn_新闻（世界）', 'seoTitle': '', 'seoDescription': '', 'meta': {'salary': 6062, 'location': 'Beijing'}}, {'langCode': 'zh-tw', 'title': '以色列作為英國和盟國的加沙市轟炸加沙市要求採取行動，以防止“展開飢荒”', 'body': '該地區的哈馬斯民防機構說，加沙市受到強烈的空襲，以色列部隊準備發動行動來接管這座城市。 發言人馬哈茂', 'summary': '這些國家要求“立即，永久和具體的步驟”，以促進向加沙的援助。', 'categoryCodes': 'zh-tw_新聞（世界）', 'seoTitle': '', 'seoDescription': '', 'meta': {'salary': 8670, 'location': 'London'}}, {'langCode': 'en', 'title': 'Israel bombards Gaza City as UK and allies demand ', 'body': 'Gaza City has come under intense air attack, the t', 'summary': 'The countries demanded "immediate, permanent and c', 'categoryCodes': 'en_News (World)', 'seoTitle': '', 'seoDescription': '', 'meta': {'salary': 7254, 'location': 'New York'}}, {'langCode': 'km', 'title': 'អ៊ីស្រាអែលបានទម្លាក់គ្រាប់បែកទីក្រុងហ្គាហ្សាជាចក្រ', 'body': 'ទីភ្នាក់ងារការពារប្រទេសអ៊ីស្រាអែលបាននិយាយថាទីក្រុង', 'summary': 'ប្រទេសនេះបានទាមទារឱ្យមានជំហានអចិន្ត្រៃយ៍អចិន្រ្តៃយ', 'categoryCodes': 'km_ព័ត៌មាន (ពិភពលោក)', 'seoTitle': '', 'seoDescription': '', 'meta': {'salary': 9804, 'location': 'Beijing'}}, {'langCode': 'th', 'title': 'อิสราเอลทิ้งระเบิดเมืองกาซาในฐานะสหราชอาณาจักรและพ', 'body': 'เมืองกาซาได้เข้ามาภายใต้การโจมตีทางอากาศอย่างรุนแร', 'summary': 'ประเทศต่างๆเรียกร้อง "ขั้นตอนทันที, ถาวรและเป็นรูป', 'categoryCodes': 'th_ข่าว (โลก)', 'seoTitle': '', 'seoDescription': '', 'meta': {'salary': 7242, 'location': 'London'}}, {'langCode': 'ms', 'title': 'Israel membombardir Gaza City sebagai UK dan Sekut', 'body': 'Gaza City telah mengalami serangan udara yang seng', 'summary': 'Negara -negara menuntut "langkah -langkah segera, ', 'categoryCodes': 'ms_Berita (Dunia)', 'seoTitle': '', 'seoDescription': '', 'meta': {'salary': 3029, 'location': 'New York'}}], 'medias': [{'mediaType': 'image', 'url': 'https://ichef.bbci.co.uk/news/1536/cpsprodpb/02bc/live/1a0a6650-777a-11f0-b15a-09fa5f596b3a.jpg.webp', 'sort': 1, 'altText': ''}, {'mediaType': 'image', 'url': 'https://ichef.bbci.co.uk/news/1536/cpsprodpb/06ba/live/75958740-7778-11f0-b15a-09fa5f596b3a.jpg.webp', 'sort': 2, 'altText': ''}]}]
    headers = sendHeaderGenerate(param)
    response = requests.post(url, headers=headers, json=param)
    print("Response status:", response.status_code)
    print("Response body:", response.text)
import asyncio
import datetime
import json
import os
import sqlite3

import pandas as pd
import requests

from tools.f2_gen_data import fake_cms_json
from tools.util import file_util as fileUtil

def excel_import():
    _dir = os.path.dirname(__file__)
    file_path = os.path.join(_dir, 'geturl.xlsx')
    excel_file = pd.ExcelFile(file_path)
    # 获取所有工作表的名称
    sheet_names = excel_file.sheet_names
    # 遍历每个工作表并读取数据

    # 读取工作表数据
    df = excel_file.parse("Sheet1", header=None)
    num_columns = len(df.columns)
    file = os.path.join(_dir, 'f0_result.txt')
    fileUtil.empty_csv(file)
    header = []
    for rowIndex, row in df.iterrows():
        result = []
        for colIndex in range(num_columns):
            if rowIndex == 0:
                header.append(row[colIndex])
            else:
                col = str(row[colIndex])
                if col == "nan" or col == None or ("'" in col) or (" " in col) or ("\n" in col):
                    continue
                obj = f"insert into tmp_kw(name,category) values('{row[colIndex]}','{header[colIndex]}');"
                result.append(obj)
        try:
            fileUtil.write_lst_csv(result, file, 0)
        except Exception as ex:
            print(str(rowIndex) + str(result))
            break

def excel_to_sqlite(excel_path, sqlite_path):
    """
     将 Excel 文件中每个 sheet 的数据导入到 SQLite 数据库中，
     每个 sheet 对应一个表，表名为 sheet 名，第一行为列名。

     :param excel_path: Excel 文件路径
     :param sqlite_path: SQLite 数据库文件路径
     """
    # 读取 Excel 文件所有 sheet
    xls = pd.ExcelFile(excel_path)

    # 连接 SQLite 数据库
    conn = sqlite3.connect(sqlite_path)

    for sheet_name in xls.sheet_names:
        # 读取当前 sheet，第一行为列名
        df = pd.read_excel(xls, sheet_name=sheet_name)

        # 写入 SQLite，表名为 sheet 名，存在则替换
        df.to_sql(sheet_name, conn, if_exists='replace', index=False)
        print(f"已导入表：{sheet_name}")

    conn.close()
    print("所有 sheet 导入完成")

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
        return result["data"]
    except Exception as ex:
        print(ex)

async def cms_import_excel():
    # excel_to_sqlite("test.xlsx","test.db")

    # 读取数据
    conn = sqlite3.connect("test.db")
    conn.row_factory = sqlite3.Row  # 以字典形式获取行数据
    cursor = conn.cursor()
    cursor.execute('''
select b.content_id,a.content_type, a.status, a.region_code, a.is_featured, a.publish_time
     ,b.lang_code, b.title, b.body, b.summary, b.seo_title, b.seo_description
     ,c.meta
from content a
inner join content_lang b on a.id=b.content_id
left join meta c on b.content_id=c.content_id and b.lang_code=c.lang_code;
''')
    rows = cursor.fetchall()
    rows = [dict(row) for row in rows]

    # 导入数据
    arr = []
    for row in rows:
        cursor.execute('''
select media_type,url,sort,alt_text from media where content_id=?;
''', (row["content_id"],))
        media_rows = cursor.fetchall()
        media_rows = [dict(row) for row in media_rows]
        arr_media = []
        for media_row in media_rows:
            arr_media.append({
                "mediaType": media_row["media_type"],
                "url": media_row["url"],
                "sort": media_row["sort"],
                "altText": media_row["alt_text"],
            })
        obj = {
            "contentType": row["content_type"],
            "status": 1 if row["status"] == "published" else 0,
            "regionCode": row["region_code"],
            "isFeatured": row["is_featured"],
            "publishTime": datetime.datetime.strptime(row["publish_time"], "%Y-%m-%d %H:%M:%S.%f").strftime(
                "%Y-%m-%d %H:%M:%S"),
            "langCode": row["lang_code"],
            "title": row["title"],
            "body": row["body"],
            "summary": row["summary"],
            "seoTitle": row["seo_title"],
            "seoDescription": row["seo_description"],
            "meta": json.loads(row["meta"]),
            "medias": arr_media
        }
        arr.append(obj)

    url = "http://a6ac7438791f7462895472cade21e351-8e8ffc566269d199.elb.ap-southeast-1.amazonaws.com:8800/cms/content/importContents"
    req_api(url, arr)

    cursor.close()
    conn.close()



if __name__ == "__main__":
    # excel_import()
    asyncio.run(cms_import_excel())
    # excel_to_sqlite("test.xlsx","test.db")

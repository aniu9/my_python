import csv
import os
import signal
from datetime import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup


def verify_telegram_urls(url, urlsNew, urlsAll, validUrls):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        cookies = {
            # 在这里添加需要的Cookies
        }
        response = requests.get(url, headers=headers, cookies=cookies, timeout=10)
        if response.status_code != 200:
            print({"error": f"Status code: {response.status_code}"})
            return

        soup = BeautifulSoup(response.content, 'html.parser')
        links = []
        for link in soup.find_all('a'):
            href = link.get('href')
            if href is None:
                continue
            if '/t.me/' in href and len(href) < 100:
                links.append([href])
            elif href.startswith("https://tgstat.ru/en/chat/@"):
                href = "https://t.me/" + href.replace("https://tgstat.ru/en/chat/@", "")
                links.append([href])
            elif href.startswith("https://ttttt.me/"):
                href = "https://t.me/" + href.split("/")[3]
                links.append([href])
            elif href.startswith("http") and len(href) < 200 and href not in urlsAll:
                urlsNew.append(href)
                urlsAll.append(href)

        # workbook = openpyxl.load_workbook('url.xlsx')
        # sheet = workbook.active
        # for row in links:
        #     sheet.append(row)
        # workbook.save('url.xlsx')
        if len(links) > 0:
            validUrls.append([url])
            cur_dir = os.path.dirname(__file__)
            file_tg_url = os.path.join(cur_dir, 'f4_result.csv')
            with open(file_tg_url, "a", newline="") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(links)

    except Exception as e:
        return


def readTodoUrl():
    # 从csv文件中读取URL
    # _dir = os.path.dirname(__file__)  启动程序目录
    # 当前程序文件所在目录
    cur_dir = os.path.dirname(__file__)
    file_path = os.path.join(cur_dir, 'f4_todo_url.csv')
    urls = []
    try:
        df = pd.read_csv(file_path, header=None)
        urls = df.iloc[:, 0].tolist()  # 读取第一列的URL
    except Exception as e:
        print(f"Error reading URLs from Excel: {e}")
    return urls


class TimeoutException(Exception):
    pass


def timeout_handler(signum, frame):
    raise TimeoutException("操作超时")


if __name__ == "__main__":
    # with open("f4_result.csv", "w", newline="") as csvfile:
    #     writer = csv.writer(csvfile)
    #     writer.writerow("")
    i = 0
    urls = readTodoUrl()
    urlsNew = []
    urlsAll = readTodoUrl()
    validUrls = []
    # 注册超时信号处理器
    signal.signal(signal.SIGALRM, timeout_handler)
    cur_dir = os.path.dirname(__file__)
    file_cj_todo = os.path.join(cur_dir, 'f4_cj_todo_url.csv')
    while i < 3:
        i = i + 1
        cnt = 0
        for url in urls:
            try:
                signal.alarm(5)
                verify_telegram_urls(url, urlsNew, urlsAll, validUrls)
                cnt = cnt + 1
                print(f"{datetime.now()},i={i},total={len(urls)},cnt={cnt}")
            except TimeoutException:
                # 操作超时，跳过并继续执行后续操作
                continue
            finally:
                # 取消超时信号
                signal.alarm(0)

        urls = urlsNew
        with open(file_cj_todo, "a", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(validUrls)
            validUrls = []
        urlsNew = []
    print(f"{datetime.now()},结束")

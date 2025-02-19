from datetime import datetime

import requests
from bs4 import BeautifulSoup


def verify_telegram_urls(url):
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
        elements = soup.find_all('div', class_="currency-table-name")
        data = []
        for elem in elements:
            text = elem.get_text().strip()
            print(text)
            data.append([text])

        # cur_dir = os.path.dirname(__file__)
        # file_tg_url = os.path.join(cur_dir, 'f4_result.csv')
        # with open(file_tg_url, "w", newline="") as csvfile:
        #     writer = csv.writer(csvfile)
        #     writer.writerows(data)

    except Exception as e:
        return

if __name__ == "__main__":
    url = "https://www.amz123.com/tools-currency"
    verify_telegram_urls(url)

    print(f"{datetime.now()},结束")

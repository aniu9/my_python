import concurrent
import csv
import datetime
import json
import os
import ssl
from concurrent.futures import ThreadPoolExecutor

import pika
import requests


def getChannel():
    # SSL Context for TLS configuration of Amazon MQ for RabbitMQ
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    ssl_context.set_ciphers('ECDHE+AESGCM:!ECDSA')

    url = f"amqps://cjssdevt:zybfeq-jymcoh-poswI3@b-359a40d8-2c5d-467d-9f7c-da502e097ecb.mq.ap-southeast-1.amazonaws.com:5671"
    parameters = pika.URLParameters(url)
    parameters.ssl_options = pika.SSLOptions(context=ssl_context)
    try:
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        return channel
        # print("Connected to RabbitMQ")
    except Exception as e:
        raise Exception(f"getChannel Error: {e}")


def get_message(channel, queue):
    method_frame, header_frame, body = channel.basic_get(queue)
    if method_frame:
        print(method_frame, header_frame, body)
        channel.basic_ack(method_frame.delivery_tag)
        return method_frame, header_frame, body
    else:
        print('No message returned')


def close(channel):
    connection = channel.connection
    channel.close()
    connection.close()


def consume_messages(queue):
    root_dir = os.path.dirname(os.path.dirname(__file__))
    file_tg_url = os.path.join(root_dir, 'supply', 'f2_msg_url.csv')

    def callback1(ch, method, properties, body):
        print(queue + " [x] Received %r" % body)
        with open(file_tg_url, "a", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([json.loads(body.decode('utf-8'))["url"]])

    channel = getChannel()
    queue_info = channel.queue_declare(queue=queue, passive=True)
    message_count = queue_info.method.message_count
    if message_count > 0:
        channel.basic_consume(queue=queue, on_message_callback=callback1, auto_ack=True)
        print(queue)
        channel.start_consuming()
    close(channel)


def handCsv():
    root_dir = os.path.dirname(os.path.dirname(__file__))
    file_tg_url = os.path.join(root_dir, 'supply', 'f2_msg_url.csv')
    with open(file_tg_url, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow("")
    with ThreadPoolExecutor(max_workers=1) as executor:
        tasks = [executor.submit(consume_messages, "cms_msg_supply_queue")]
        # 等待所有任务完成
        concurrent.futures.wait(tasks)
    print("csv已处理完")


def handApiCsv():
    root_dir = os.path.dirname(os.path.dirname(__file__))
    file_tg_url = os.path.join(root_dir, 'supply', 'f2_msg_url.csv')
    # with open(file_tg_url, "w", newline="") as csvfile:
    #     writer = csv.writer(csvfile)
    #     writer.writerow("")

    url = "http://a5b684db7f9d746b18f60bbe60781a74-8b898802e7cba134.elb.ap-southeast-1.amazonaws.com:8800/cms/content/getSupplyMsgs"
    headers = {
        "appKey": "90b0e5c2eb0d11eeba8ee49d6628936c",
        "Content-Type": "application/json"
    }
    i = 0
    count = 0
    while i < 1000:
        i = i + 1
        response = requests.post(url, headers=headers, json={"size": 500})
        if response.status_code != 200:
            print({"error": f"Status code: {response.status_code}"})
            return
        lst_data = json.loads(response.content)["data"]
        if lst_data is None or len(lst_data) == 0:
            return

        count = count + len(lst_data)
        print(f"{datetime.datetime.now()},count:{count}")
        lst_data = [[item["url"]] for item in lst_data]
        with open(file_tg_url, "a", newline="") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(lst_data)


def getData(size):
    url = "http://a5b684db7f9d746b18f60bbe60781a74-8b898802e7cba134.elb.ap-southeast-1.amazonaws.com:8800/cms/content/getSupplyMsgs"
    headers = {
        "appKey": "90b0e5c2eb0d11eeba8ee49d6628936c",
        "Content-Type": "application/json"
    }
    response = requests.post(url, headers=headers, json={"size": size})
    if response.status_code != 200:
        print({"error": f"Status code: {response.status_code}"})
        return []
    lst_data = json.loads(response.content)["data"]
    urls = []
    for data in lst_data:
        urls.append(data["url"])
    return urls


if __name__ == "__main__":
    handApiCsv()

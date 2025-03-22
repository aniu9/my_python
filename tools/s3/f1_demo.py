from enum import unique, Enum

import requests
import json

@unique
class BsType(Enum):
    long_listen = 0

def get_presigned_url(lambda_url, filename):
    """
    调用 Lambda 函数获取预签名 URL
    """
    params = {
        'filename': filename,
        'contentType': "application/json"
    }
    response = requests.get(lambda_url, params=params)
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        raise Exception(f"Failed to get presigned URL: {response.text}")

def upload_file():
    # Lambda 函数的 URL（通过 API Gateway 暴露）
    lambda_url = "https://qcjzkevx442nsv72zsdosyyo6a0lbhon.lambda-url.ap-southeast-1.on.aws/"
    # lambda_url = "https://wkplvul4qc4p45lox3khamzpeq0jziab.lambda-url.ap-southeast-1.on.aws/"

    # 要上传的文件信息
    # 群消息采集
    # filename = "cms/collect/2025/01/08/07/msg_c_20250107154101.json"
    # 网络爬虫
    # filename = "cms/collect/2025/01/07/19/emptylink_c_20250106154100.json"
    # 群信息采集
    # filename = "cms/collect/2025/01/08/07/group_u_20250106154100.json"
    # 消息信息补充
    filename = "cms/collect/2025/01/08/07/msg_u_20250106154100.json"

    # 本地文件路径
    file_path = "/Users/Work/temp/json1.json"

    try:
        # 获取预签名 URL
        result = get_presigned_url(lambda_url, filename)
        presigned_url = result['presigned_url']
        # presigned_url="https://userpostmedia.s3.amazonaws.com/cms/collect/msg_info_20250106154102.json?AWSAccessKeyId=ASIA3FLD4HR636YTCMDF&Signature=lwGPVq10Iw2bqEvAwCki88yd9Is%3D&content-type=application%2Fjson&x-amz-security-token=IQoJb3JpZ2luX2VjEHAaDmFwLXNvdXRoZWFzdC0xIkYwRAIgQBMCLMIvTA3LEHy7DOp2lkka12bGGMmClunv7f%2FC9yACIBWIKv2ZtR2KwhzLnoRx9EU861OdBjHgcwf66fchGMkmKukCCFkQABoMNzY3Mzk4MDAxNzg5IgzNGKnZjlNPU7N8S8YqxgKipJXZQZURhDZoT%2FNx89RPB%2FZIWKLxl6OeYydAhu0Gs2UguZcEU6sYSQ81yJfPoc%2FL5%2FMgO8bvaQ%2FW3S33scL%2FQNgoZv%2FYHeiwtP%2B0C3GoGdS96zW3kUxdq3uzrYOvHu7gacQVfClSykuNHrsgK9mHrTnoMF1vQYA0Y5sVlfFWonDRu4NGmpZUsfRHhilHDsx%2BvdaCtMnuNO9n7EoX%2FoljAB0dQaTAjlqP9DRS%2BQOyf4odZhp%2Big2MUaAaZmfuVN%2F2kEakmjaVYk1i9gb6ifF4O%2FZs1M4Xe%2Bu8z6IRq64g7jb6z7NklfvSMKVkmGU5HeJbeSEpmwEQFwhkDSwuWWG3kVpeKLU52iwB7zQzjjJ5kAMqLdKfZYlAt74oa6X%2BCLYPOZ2ZBQl%2BzQIU19W1zoE8ubWQTafREcEFgWI4n5O%2B2re6FiRsJjCKt%2FO7BjqfARm04aF%2Bn9VeJsCOFXCNVuJDQT4TrTOUwxJ4x9ZONogiiFDylegVQF9w6QWc7PaQayOVGAiJRAKZ1jZw1tjaz%2B91EF1CH0hPPfot5vxEgOlsXZ3rJ8DqRDkazek5JHWGohdeg9R%2BxeXrPQFU3Il2S58zww%2B1%2FaeQ3C6lCCKjvWtY%2BLcQlhTqtnC0VWaF3DDW4Gwfg3IWJKdZXa0RcvDjOw%3D%3D&Expires=1736235977"
        print(f"Got presigned URL: {presigned_url}")

        # 使用预签名 URL 上传文件
        with open(file_path, 'rb') as file:
            headers = {'Content-Type': "application/json; charset=utf-8"}
            response = requests.put(presigned_url, data=file, headers=headers)
            if response.status_code == 200:
                print(f"File uploaded successfully to {result['bucket_name']}/{result['file_name']}")
            else:
                print(f"Failed to upload file. Status code: {response.status_code}")
                print(f"Response: {response.text}")

    except Exception as e:
        print(f"An error occurred: {str(e)}")

def down_file():
    # 调用 Lambda 函数获取预签名 URL
    params = {
        'bsType': BsType.long_listen.value
    }
    # getPendingData
    lambda_url = "https://n3hsntfhqrnb73ltxrkum4smiy0mgkhy.lambda-url.ap-southeast-1.on.aws/"
    response = requests.get(lambda_url, params=params)
    presigned_url = ""
    if response.status_code == 200:
        presigned_url = json.loads(response.text)['presigned_url']
    elif response.status_code == 404:
        print("\n完成")
        return
    elif response.status_code == 500:
        print(response.text)
        return
    else:
        print(response.reason)
        return

    # 使用预签名拉取数据
    headers = {'Content-Type': "application/json; charset=utf-8"}
    response = requests.get(presigned_url)
    if response.status_code == 200:
        json_data = json.loads(response.content)
        print(json_data)
    else:
        print(response.reason)

# 使用示例
if __name__ == "__main__":
    down_file()

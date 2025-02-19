import os
import re

import pandas as pd
from tools.util import file_util as fileUtil
import json
import boto3
from botocore.exceptions import ClientError
import logging

# 创建 S3 客户端
s3 = boto3.client('s3')

# 或者创建 S3 资源对象
# s3 = boto3.resource('s3')

_cur_dir = os.path.dirname(__file__)
_root_dir = os.path.dirname(_cur_dir)

def upload_file(file_name, bucket, object_name=None):
    if object_name is None:
        object_name = file_name

    try:
        s3.upload_file(file_name, bucket, object_name)
    except Exception as e:
        print(f"Upload failed: {e}")
        return False
    return True

def download_file(bucket, object_name, file_name):
    try:
        s3.download_file(bucket, object_name, file_name)
    except Exception as e:
        print(f"Download failed: {e}")
        return False
    return True

def list_objects(bucket):
    try:
        response = s3.list_objects_v2(Bucket=bucket)
        return [obj['Key'] for obj in response.get('Contents', [])]
    except Exception as e:
        print(f"List objects failed: {e}")
        return []

def delete_object(bucket, object_name):
    try:
        s3.delete_object(Bucket=bucket, Key=object_name)
    except Exception as e:
        print(f"Delete failed: {e}")
        return False
    return True

import json

def set_bucket_policy(bucket_name, policy):
    try:
        s3.put_bucket_policy(Bucket=bucket_name, Policy=json.dumps(policy))
    except Exception as e:
        print(f"Set policy failed: {e}")
        return False
    return True

    # # 使用示例
    # policy = {
    #     "Version": "2012-10-17",
    #     "Statement": [
    #         {
    #             "Sid": "PublicReadGetObject",
    #             "Effect": "Allow",
    #             "Principal": "*",
    #             "Action": "s3:GetObject",
    #             "Resource": f"arn:aws:s3:::{bucket_name}/*"
    #         }
    #     ]
    # }
    # set_bucket_policy('my-bucket', policy)



def save_json_to_s3(json_data, bucket_name, object_name):
    """
    将 JSON 数据保存到 S3 桶中

    :param json_data: 要保存的 JSON 数据（可以是字典或列表）
    :param bucket_name: S3 桶名称
    :param object_name: 要保存的对象名称（S3 中的文件路径）
    :return: 如果保存成功返回 True，否则返回 False
    """
    # 创建 S3 客户端
    s3_client = boto3.client('s3')

    try:
        # 将 JSON 数据转换为字符串
        json_string = json.dumps(json_data)

        # 将 JSON 字符串上传到 S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_name,
            Body=json_string,
            ContentType='application/json'
        )
        print(f"JSON data successfully saved to {bucket_name}/{object_name}")
        return True
    except ClientError as e:
        print(f"Error saving JSON data to S3: {e}")
        return False


def save_json_to_s3_directory(json_data, bucket_name, directory, file_name):
    """
    将 JSON 数据保存到 S3 桶的指定目录中

    :param json_data: 要保存的 JSON 数据（可以是字典或列表）
    :param bucket_name: S3 桶名称
    :param directory: S3 中的目录路径（不要以 '/' 开头）
    :param file_name: 文件名
    :return: 如果保存成功返回 True，否则返回 False
    """

    # 创建 S3 客户端
    s3_client = boto3.client('s3')

    # 构建完整的对象键（S3 中的文件路径）
    object_key = os.path.join(directory, file_name).replace("\\", "/")

    try:
        # 将 JSON 数据转换为字符串
        json_string = json.dumps(json_data, indent=2)  # 使用 indent 使 JSON 更易读

        # 将 JSON 字符串上传到 S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=json_string,
            ContentType='application/json'
        )
        logger.info(f"JSON data successfully saved to {bucket_name}/{object_key}")
        return True
    except ClientError as e:
        logger.error(f"Error saving JSON data to S3: {e}", exc_info=True)
        return False

# 使用示例
def demo():
    # 示例 JSON 数据
    sample_data = {
        "name": "John Doe",
        "age": 30,
        "city": "New York",
        "interests": ["reading", "hiking", "photography"]
    }

    # S3 配置
    bucket_name = 'userpostmedia'
    directory = 'geturl'  # 指定目录
    file_name = 'file.json'

    # 保存 JSON 数据到 S3 的指定目录
    success = save_json_to_s3_directory(sample_data, bucket_name, directory, file_name)

    if success:
        print("Operation completed successfully.")
    else:
        print("Operation failed.")

# 使用示例
# if __name__ == "__main__":
#     # 示例 JSON 数据
#     sample_data = {
#         "name": "John Doe",
#         "age": 30,
#         "city": "New York",
#         "interests": ["reading", "hiking", "photography"]
#     }
#
#     # S3 桶名称和对象名称
#     bucket_name = 'your-bucket-name'
#     object_name = 'path/to/your/file.json'
#
#     # 保存 JSON 数据到 S3
#     success = save_json_to_s3(sample_data, bucket_name, object_name)
#
#     if success:
#         print("Operation completed successfully.")
#     else:
#         print("Operation failed.")
#
# if __name__ == "__main__":
#     # 使用示例
#     upload_file('/Users/Work/temp/test1.json', 'my-bucket', 'test1.json')
#     # 使用示例
#     download_file('my-bucket', 'remote_file.txt', 'downloaded_file.txt')
#     # 使用示例
#     objects = list_objects('my-bucket')
#     for obj in objects:
#         print(obj)
#     # 使用示例
#     delete_object('my-bucket', 'remote_file.txt')

if __name__ == "__main__":
    # 创建 STS 客户端
    sts_client = boto3.client('sts')

    # 定义角色 ARN 和会话名称
    role_arn = "arn:aws:iam::767398001789:role/cjss-sit-eks-worker-node-role"
    role_session_name = "AssumedRoleSession"

    try:
        # 调用 assume_role 方法
        assumed_role_object = sts_client.assume_role(
            RoleArn=role_arn,
            RoleSessionName=role_session_name
        )

        # 从返回的对象中提取临时凭证
        credentials = assumed_role_object['Credentials']

        print("临时凭证获取成功:")
        print(f"Access Key: {credentials['AccessKeyId']}")
        print(f"Secret Key: {credentials['SecretAccessKey']}")
        print(f"Session Token: {credentials['SessionToken']}")
        print(f"Expiration: {credentials['Expiration']}")

        # 使用这些临时凭证创建一个新的 S3 客户端
        s3_client = boto3.client(
            's3',
            aws_access_key_id=credentials['AccessKeyId'],
            aws_secret_access_key=credentials['SecretAccessKey'],
            aws_session_token=credentials['SessionToken']
        )

        # 现在您可以使用 s3_client 来访问 S3 资源
        # 例如，列出桶
        response = s3_client.list_buckets()
        print("\n可访问的 S3 桶:")
        for bucket in response['Buckets']:
            print(f"- {bucket['Name']}")

    except Exception as e:
        print(f"错误: {str(e)}")
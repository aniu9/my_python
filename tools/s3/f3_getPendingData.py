import json
import boto3
from botocore.exceptions import ClientError
from operator import itemgetter

def lambda_handler(event, context):
    # 从环境变量获取 S3 桶名
    bucket_name = 'userpostmedia'

    # 从事件中获取参数
    bs_type = event.get('queryStringParameters', {}).get('bsType', f'')

    # bs_type：0长监听
    path = ''
    if bs_type=='0':
        path = 'cms/pending_data/long_listen/'
    else:
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json; charset=utf-8'},
            'body': json.dumps('不识别的业务类型:{}'.format(bs_type))
        }

    # 创建 S3 客户端
    s3_client = boto3.client('s3')

    try:
        # 列出指定目录下的所有对象
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=path
        )

        def is_file(s3_object):
            return not s3_object['Key'].endswith('/')
        # 过滤出所有文件
        files = [obj for obj in response['Contents'] if is_file(obj)]

        # 检查是否有文件
        if not files:
            return {
                'statusCode': 404,
                'headers': {'Content-Type': 'application/json; charset=utf-8'},
                'body': json.dumps('No files found in the specified directory')
            }

        # 按最后修改时间排序，获取最旧的文件
        oldest_file = min(files, key=itemgetter('LastModified'))
        oldest_file_key = oldest_file['Key']

        # 生成预签名 URL
        presigned_url = s3_client.generate_presigned_url('get_object',
                                                         Params={'Bucket': bucket_name,
                                                                 'Key': oldest_file_key},
                                                         ExpiresIn=500,
                                                         HttpMethod='GET')

        return {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json; charset=utf-8','Access-Control-Allow-Origin': '*'},
            'body': json.dumps({
                'oldest_file_key': oldest_file_key,
                'presigned_url': presigned_url
            })
        }
    except ClientError as e:
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json; charset=utf-8'},
            'body': json.dumps('Error generating presigned URL')
        }
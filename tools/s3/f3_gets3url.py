import json
import uuid
import boto3
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    # 从环境变量获取 S3 桶名
    bucket_name = 'userpostmedia'

    # 从事件中获取参数
    bs_type = event.get('queryStringParameters', {}).get('bsType', f'')

    # bs_type：group_c群采集 msg_c消息采集 group_u群更新 msg_u消息更新
    prefix = 'cms/collect/2025/01/08/07/'
    suffix = '_20250107154101.json'
    file_name = ""
    if bs_type == 'group_c' or bs_type == 'msg_c' or bs_type == 'group_u' or bs_type == 'msg_u':
        file_name = prefix + bs_type + suffix
    else:
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json; charset=utf-8'},
            'body': json.dumps('不识别的业务类型:{}'.format(bs_type))
        }

    # 创建 S3 客户端
    s3_client = boto3.client('s3')

    try:
        # 生成预签名 URL 用于上传（PUT）操作
        url = s3_client.generate_presigned_url(
            'put_object',
            Params={
                'Bucket': bucket_name,
                'Key': file_name,
                'ContentType': 'application/json; charset=utf-8'
            },
            ExpiresIn=300,  # URL 有效期
            HttpMethod='PUT'
        )

        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',  # 允许跨域请求，根据需要调整
                'Content-Type': 'application/json; charset=utf-8'
            },
            'body': json.dumps({
                'presigned_url': url,
                'file_name': file_name,
                'message': 'Use this URL to upload the file via a PUT request.'
            })
        }
    except ClientError as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

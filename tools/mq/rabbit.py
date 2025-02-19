import ssl

import pika


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


def consume_messages(queue, callback):
    def callback1(ch, method, properties, body):
        print(queue + " [x] Received %r" % body)
        callback(body.decode('utf-8'))

    channel = getChannel()
    # queue_info = channel.queue_declare(queue=queue, passive=True)
    # message_count = queue_info.method.message_count
    # if message_count > 0:
    channel.basic_consume(queue=queue, on_message_callback=callback1, auto_ack=True)
    print(queue)
    channel.start_consuming()
    close(channel)

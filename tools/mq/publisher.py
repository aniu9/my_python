from datetime import datetime

from rabbit import BasicPikaClient


class BasicMessageSender(BasicPikaClient):
    def declare_queue(self, queue_name):
        print(f"Trying to declare queue({queue_name})...")
        try:
            self.channel.queue_declare(queue=queue_name)
            print(f"Queue({queue_name}) declared.")
        except Exception as e:
            print(f"Error: {e}")

    def send_message(self, exchange, routing_key, body):
        try:
            channel = self.connection.channel()
            channel.basic_publish(exchange=exchange,
                                routing_key=routing_key,
                                body=body)
            print(f"Sent message. Exchange: {exchange}, Routing Key: {routing_key}, Body: {body}")
        except Exception as e:
            print(f"Error: {e}")

    def close(self):
        self.channel.close()
        self.connection.close()

if __name__ == "__main__":
    # Initialize Basic Message Sender which creates a connection
    # and channel for sending messages.
    try:
        basic_message_sender = BasicMessageSender(
            "b-bffd6fd2-c7df-4300-864b-2544c0f308a4",
            "cjss_botpclient",
            "FXaLwWZkMFTs1Z9GwBTk",
            "ap-southeast-1"
        )
        # Declare a queue
        basic_message_sender.declare_queue("cms_queue")

        # Send a message to the queue.
        msg_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        basic_message_sender.send_message(exchange="", routing_key="cms_queue", body=b'Hello CJSS! '+msg_time.encode('utf-8'))

        # Close connections.
        basic_message_sender.close()
    except Exception as e:
        print(f"Error: {e}")

    
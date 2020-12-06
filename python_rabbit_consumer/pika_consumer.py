import pika
from consumer_settings import pika_connection
from consumer_settings import pika_queue
from consumer_settings import json_file_consumer

def callback(ch, method, props, body):
    body_str = body.decode()
    print('body_str: ', body_str)
    ch.basic_ack(delivery_tag = method.delivery_tag)

def main():
    with pika.BlockingConnection(pika.ConnectionParameters(pika_connection)) as connection:
        channel = connection.channel()
        channel.queue_declare(queue=pika_queue, durable=True)
        channel.basic_qos(prefetch_count=1) #tells only one message per worker, instead assigns message to a free worker
        channel.basic_consume(queue=pika_queue, on_message_callback=callback)
        print('accepting incomming messages from rabbitMQ now')
        channel.start_consuming()


if __name__ == "__main__":
    main()

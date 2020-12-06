import pika
import json
from consumer_settings import pika_connection
from consumer_settings import pika_queue
from consumer_settings import json_file_consumer

class queue_client():

    def __init__(self):
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=pika_connection))

        self.channel = self.connection.channel()

        result = self.channel.queue_declare(queue=pika_queue, durable=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body
    
    def call(self, data):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key=pika_queue,
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
                delivery_mode=2,  # make message persistent
            ),
            body=f"""{data}""")
        while self.response is None:
            self.connection.process_data_events()
        return self.response

def main():
    consumer_data = json.load(open(json_file_consumer))
    queue_user = queue_client()
    response = queue_user.call(consumer_data)
    print('response: ', response.decode())
    
if __name__ == "__main__":
    main()

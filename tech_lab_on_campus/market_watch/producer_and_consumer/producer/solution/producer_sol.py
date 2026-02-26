import pika
from producer_interface import mqProducerInterface

class mqProducer(mqProducerInterface):
    def __init__(self, routing_key, exchange_name):
        self.routing_key = routing_key
        self.exchange_name = exchange_name
        self.channel = None
        self.connection = None
        self.setupRMQConnection()

    def setupRMQConnection(self):
        credentials = pika.PlainCredentials('guest', 'guest')
        
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host='localhost',
                port=5672,
                credentials=credentials
            )
        )
        self.channel = self.connection.channel()
        self.channel.exchange_declare(
            exchange=self.exchange_name,
            exchange_type='direct',
            durable=True
        )
        self.channel.queue_declare(queue=self.routing_key)
        self.channel.queue_bind(
            exchange=self.exchange_name,
            queue=self.routing_key,
            routing_key=self.routing_key
        )

    def publishOrder(self, message):
        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=self.routing_key,
            body=message
        )

        self.channel.close()
        self.connection.close()


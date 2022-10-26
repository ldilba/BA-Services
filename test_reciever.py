import json

import pika
import requests


def callback(ch, method, properties, body: bytes):
    received = json.loads(body.decode('utf-8'))

    print(received)
    requests.post("http://localhost:80/receive",
                  json={"uid": received["uid"], "service": received["service"], "message": "Hier ist die Response :)"})


if __name__ == '__main__':
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='text-similarity')

    channel.basic_consume(queue='text-similarity',
                          auto_ack=True,
                          on_message_callback=callback)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

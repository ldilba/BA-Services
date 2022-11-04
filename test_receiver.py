import json

import pika
import requests
import os


def callback(ch, method, properties, body: bytes):
    received = json.loads(body.decode('utf-8'))
    print(received)
    cookies = {}

    url = os.environ.get('FLASK_URL')
    r = requests.post(url + "/receive", cookies=cookies,
                      json={"uid": received["uid"], "service": received["service"],
                            "message": "Why you should never center a div."})


if __name__ == '__main__':
    credentials = pika.PlainCredentials('guest', 'guest')
    rabbit_host = os.environ.get('RABBIT_HOST')
    connection = pika.BlockingConnection(pika.ConnectionParameters(rabbit_host, 5672, '/', credentials))
    channel = connection.channel()

    channel.queue_declare(queue='text-similarity')

    channel.basic_consume(queue='text-similarity',
                          auto_ack=True,
                          on_message_callback=callback)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

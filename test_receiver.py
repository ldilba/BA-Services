import json

import pika
import requests
import os


def respond(uid, service):
    connection = pika.BlockingConnection(pika.ConnectionParameters(rabbit_host, 5672, '/', credentials))
    channel = connection.channel()
    channel.queue_declare(queue='response-text-similarity')
    message = '{"title": "How to stop an interval on an Observable in RxJS", "link": "https://stackoverflow.com/questions/46963486/how-to-stop-an-interval-on-an-observable-in-rxjs", "content": "I have a specific situation where I\'m using an RxJS interval, but at any given moment I may need to stop that interval. I assumed there was something easy like a cancel() or stop(). Similar to clearTimeout. This is possible to stop an interval once it\'s going? If not, what would be another approach."}'

    channel.basic_publish(exchange='',
                          routing_key='response-text-similarity',
                          body=f'{{"uid": "{uid}", "service": "{service}", "message": {message}}}'.encode('utf-8'))
    channel.close()
    connection.close()


def callback(ch, method, properties, body: bytes):
    received = json.loads(body.decode('utf-8'))
    print(received)
    respond(received["uid"], received["service"])


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

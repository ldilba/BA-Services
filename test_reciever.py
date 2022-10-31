import json

import pika
import requests

session = ""


def callback(ch, method, properties, body: bytes):
    received = json.loads(body.decode('utf-8'))
    global session
    print(received)
    cookies = {}
    if session != "":
        cookies['session'] = session

    r = requests.post("http://localhost:80/receive", cookies=cookies,
                      json={"uid": received["uid"], "service": received["service"],
                            "message": "Why you should never center a div."})

    session = r.cookies['session']


if __name__ == '__main__':
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='text-similarity')

    channel.basic_consume(queue='text-similarity',
                          auto_ack=True,
                          on_message_callback=callback)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

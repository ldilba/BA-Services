import pika

if __name__ == '__main__':
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='service')

    channel.basic_publish(exchange='',
                          routing_key='service',
                          body=b'hi :3')

    print(" [x] Sent 'Hello World!'")
    connection.close()


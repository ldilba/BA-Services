import pika

if __name__ == '__main__':
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='text-similarity-response')

    channel.basic_publish(exchange='',
                          routing_key='text-similarity-response',
                          body=f'{{"uid": "2784f7fb-cd3d-41c8-8949-ba4aa8d53067", "service": "text-similarity-response",'
                               f'"response": "das ist eine test response"}}'.encode('utf-8'))

    print(" [x] Sent 'Hello World!'")
    connection.close()

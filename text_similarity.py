import json
import time

import pika
import os

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

import tensorflow as tf
import tensorflow_hub as hub


def respond(uid, service, query, params):
    connection = pika.BlockingConnection(pika.ConnectionParameters(rabbit_host, 5672, '/', credentials))
    channel = connection.channel()
    channel.queue_declare(queue='response-text-similarity')
    search_size = 1
    for p in params:
        if "search_size" in p.values():
            search_size = p["value"]

    message = handle_query(query, search_size)

    channel.basic_publish(exchange='',
                          routing_key='response-text-similarity',
                          body=f'{{"uid": "{uid}", "service": "{service}", "message": {json.dumps(message)}}}'.encode(
                              'utf-8'))
    print(f'Send: {{"uid": "{uid}", "service": "{service}", "message": {json.dumps(message)}}}')
    channel.close()
    connection.close()


def callback(ch, method, properties, body: bytes):
    received = json.loads(body.decode('utf-8'))
    print(received)
    respond(received["uid"], received["service"], received["query"], received["params"])


def handle_query(query, search_size):
    res = []
    embedding_start = time.time()
    query_vector = embed_text([query])[0]
    embedding_time = time.time() - embedding_start

    script_query = {
        "script_score": {
            "query": {"match_all": {}},
            "script": {
                "source": "cosineSimilarity(params.query_vector, doc['title_vector']) + 1.0",
                "params": {"query_vector": query_vector}
            }
        }
    }

    search_start = time.time()
    response = client.search(
        index=INDEX_NAME,
        body={
            "size": search_size,
            "query": script_query,
            "_source": {"includes": ["title", "body"]}
        }
    )
    search_time = time.time() - search_start
    print()
    print("{} total hits.".format(response["hits"]["total"]["value"]))
    print("embedding time: {:.2f} ms".format(embedding_time * 1000))
    print("search time: {:.2f} ms".format(search_time * 1000))
    for hit in response["hits"]["hits"]:
        res.append({"title": hit["_source"]["title"], "body": hit["_source"]["body"]})
    return res


def embed_text(text):
    vectors = session.run(embeddings, feed_dict={text_ph: text})
    return [vector.tolist() for vector in vectors]


def index_batch(docs):
    titles = [doc["title"] for doc in docs]
    title_vectors = embed_text(titles)

    requests = []
    for i, doc in enumerate(docs):
        request = doc
        request["_op_type"] = "index"
        request["_index"] = INDEX_NAME
        request["title_vector"] = title_vectors[i]
        requests.append(request)
    bulk(client, requests)


def index_data():
    print("Creating the 'posts' index.")
    client.indices.delete(index=INDEX_NAME, ignore=[404])

    with open(INDEX_FILE) as index_file:
        source = index_file.read().strip()
        client.indices.create(index=INDEX_NAME, body=source)

    docs = []
    count = 0

    with open(DATA_FILE) as data_file:
        for line in data_file:
            line = line.strip()

            doc = json.loads(line)
            if doc["type"] != "question":
                continue

            docs.append(doc)
            count += 1

            if count % BATCH_SIZE == 0:
                index_batch(docs)
                docs = []
                print("Indexed {} documents.".format(count))

        if docs:
            index_batch(docs)
            print("Indexed {} documents.".format(count))

    client.indices.refresh(index=INDEX_NAME)
    print("Done indexing.")


if __name__ == '__main__':
    INDEX_NAME = "posts"
    INDEX_FILE = "data/posts/index.json"

    DATA_FILE = "data/posts/posts.json"
    BATCH_SIZE = 1000

    # SEARCH_SIZE = 5

    GPU_LIMIT = 0.5
    tf.compat.v1.disable_eager_execution()
    print("Downloading pre-trained embeddings from tensorflow hub...")
    embed = hub.Module("https://tfhub.dev/google/universal-sentence-encoder/2")
    text_ph = tf.compat.v1.placeholder(tf.string)
    embeddings = embed(text_ph)
    print("Done.")

    print("Creating tensorflow session...")
    config = tf.compat.v1.ConfigProto()
    config.gpu_options.per_process_gpu_memory_fraction = GPU_LIMIT
    session = tf.compat.v1.Session(config=config)
    session.run(tf.compat.v1.global_variables_initializer())
    session.run(tf.compat.v1.tables_initializer())
    print("Done.")

    elasticsearch_url = os.environ.get('ELASTIC_URL')
    print("Connecting to " + elasticsearch_url)
    client = Elasticsearch(elasticsearch_url)

    index_data()

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

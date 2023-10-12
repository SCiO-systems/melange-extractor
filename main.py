from confluent_kafka import Producer,Consumer
import json
import melange_extractor
from dotenv import dotenv_values

config = dotenv_values(".env")  # config = {"USER": "foo", "EMAIL": "foo@example.org"}
bootstrap_server = config["bootstrap_server"]
# print(bootstrap_server)
# bootstrap_server = 'kafka.scio.services:9092'
consumer_topic = config["consumer_topic"]
producer_topic = config["producer_topic"]


def consume_messages():

    config = {
        'bootstrap.servers': bootstrap_server,
        'group.id': 'some_group',
        'auto.offset.reset': 'earliest'}
    # Create Consumer instance

    consumer = Consumer(config)

    # Subscribe to topic
    consumer.subscribe([consumer_topic])

    # Poll for new messages from Kafka and print them.
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting...")
                continue
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print("Error partition")
                    continue
                else:
                    print(f"Error while consuming message: {msg.error()}")
                    break
                print("ERROR: %s".format(msg.error()))
            else:

                # Extract the (optional) key and value, and print.
                if msg.value() == None:
                    print("Consumed event to topic {topic}: key = {key:12} value = {value:12}".format(
                        topic=msg.topic(), key=msg.key().decode('utf-8'), value=""))
                else:
                    print("Consumed event to topic {topic}: key = {key:12} value = {value:12}".format(
                        topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

                    input_msg = json.loads(msg.value())
                    embedder = melange_extractor.CiGi_Embedder(sample_json)
                    embedder.digest_input()
                    embedder.initialize_embedder()
                    embedder.embed()
                    embedder_output = json.dumps(embedder.output_data)

                    config = {
                        'bootstrap.servers': bootstrap_server  # 'bootstrap.servers': 'localhost:9092'
                    }
                    producer = Producer(config)

                    def delivery_callback(err, msg):
                        if err:
                            print('ERROR: Message failed delivery: {}'.format(err))
                            print("Failed to deliver message: %s" % (str(msg)))
                        else:
                            if msg.value() == None:
                                print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                                    topic=msg.topic(), key=msg.key().decode('utf-8'), value=""))
                            else:
                                print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                                    topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))


                    # Produce data by selecting random values from these lists.
                    producer_key = input_msg["metadata_id"]
                    producer.produce(producer_topic, key=producer_key, value=embedder_output, callback=delivery_callback)

                    producer.flush()

    except Exception as e:
        print(e)
    finally:
        # Leave group and commit final offsets
        consumer.close()


if __name__ == '__main__':
    consume_messages()
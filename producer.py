import os
import sys
import shutil
import time

from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import config


def publish_folder():
    admin_client = KafkaAdminClient(
        bootstrap_servers="localhost:29092",
        client_id='producer'
    )
    create_topic(admin_client=admin_client, topic=config['kafka_main_topic'],
                 partitions=config['model_data_loader_workers'])

    # Start up producer
    producer = KafkaProducer(bootstrap_servers=['localhost:29092'])

    code_index = 0
    code = input('Inserisci un codice identificativo del tessuto (inserisci "quit" per uscire): ')
    if code == 'quit':
        print("\nExiting.")
        producer.close()
        sys.exit(0)

    uploaded_topic_folder = os.path.join(config['uploaded_folder'], code)
    if not os.path.exists(uploaded_topic_folder):
        os.makedirs(uploaded_topic_folder)

    while True:
        try:
            # Iterate and upload images to kafka
            for index, image_filename in enumerate(os.listdir(config['input_folder'])):
                input_path = os.path.join(config['input_folder'], image_filename)

                uploaded_path = os.path.join(uploaded_topic_folder, image_filename)
                shutil.move(input_path, uploaded_path)

                msg = bytearray(f'image|{uploaded_path}|{code}|{code_index}|{index}', "utf-8")
                producer.send(config['kafka_main_topic'], msg)
                producer.flush()

                print(f'Image {uploaded_path} published')

            time.sleep(1)
        except:
            print("\nExiting.")
            producer.close()
            sys.exit(0)


def delete_topic(admin_client, topic: str):
    fs = admin_client.delete_topics(topics=[topic], timeout_ms=30)
    for tpc, f in fs.items():
        try:
            f.result()  # The result itself is None
            print(f'Topic {tpc} deleted')
        except Exception as e:
            print(f'Failed to delete topic {tpc}: {e}')


def create_topic(admin_client, topic: str, partitions: int):
    try:
        topic_list = [NewTopic(name=topic, num_partitions=partitions, replication_factor=1)]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f'topic {topic} created!')
    except KafkaError as ke:
        print(str(ke))
        pass


if __name__ == '__main__':
    publish_folder()

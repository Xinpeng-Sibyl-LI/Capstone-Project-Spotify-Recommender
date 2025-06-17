# kafka/consumer.py

from confluent_kafka import Consumer
import json
import logging
import os
from ingestion import load_to_snowflake
from ingestion.load_to_snowflake import load_spotify_data  # use your existing loader function

KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'spotify-consumer-group',
    'auto.offset.reset': 'earliest'  # or 'latest' if you only want new messages
}

TOPIC_NAME = 'spotify.tracks.raw'

def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting Kafka consumer")

    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([TOPIC_NAME])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue
            if msg.error():
                logging.error(f"Consumer error: {msg.error()}")
                continue

            record = json.loads(msg.value().decode('utf-8'))

            # Your Snowflake loader expects a list
            load_spotify_data()

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        logging.info("Kafka consumer shut down.")

if __name__ == '__main__':
    main()

"""
kafka/producer.py
────────────────────────────────────────────────────────
Fetch top-tracks data from Spotify (via ingestion.crawl)
and publish each track record to a Kafka topic.

Requires:
  • confluent-kafka
  • ingestion/crawl.py with fetch_artists_and_tracks()
  • SPOTIFY creds already handled inside crawl.py
"""

import json
import logging
from confluent_kafka import Producer
from ingestion.crawl import fetch_artists_and_tracks  # <-- your crawler wrapper

# ─── Kafka configuration ──────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = "localhost:9092"      # use 'kafka:9092' if running inside Docker
TOPIC_NAME      = "spotify.tracks.raw"

producer_conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "client.id": "spotify-producer",
    # Optional: tune batch size / linger for higher throughput
}

# ─── Delivery-report callback ─────────────────────────────────────────────────
def delivery_report(err, msg):
    if err is not None:
        logging.error("❌ delivery failed: %s", err)
    else:
        logging.info(
            "✅ delivered key=%s to %s [%d] @ offset %d",
            msg.key(),
            msg.topic(),
            msg.partition(),
            msg.offset()
        )

# ─── Main workflow ───────────────────────────────────────────────────────────
def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.info("🚀 starting Spotify → Kafka producer")

    # 1. Fetch fresh data from the crawler
    data = fetch_artists_and_tracks()            # returns {"top_artists": …, "top_tracks": …}
    tracks = data["top_tracks"]
    logging.info("Fetched %d tracks from Spotify API", len(tracks))

    # 2. Produce to Kafka
    producer = Producer(producer_conf)

    for track in tracks:
        # Use track ID as Kafka message key (helps partitioning)
        track_id = track.get("id", "")
        producer.produce(
            TOPIC_NAME,
            key=track_id,
            value=json.dumps(track).encode("utf-8"),
            callback=delivery_report,
        )
        producer.poll(0)        # trigger any queued callbacks immediately

    # 3. Wait for all messages to be delivered
    producer.flush()
    logging.info("🎉 all tracks sent; exiting")

# ─── Entry-point ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    main()

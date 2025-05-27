import os
import json
import psycopg2
import logging

from confluent_kafka import Consumer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/service.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SCORING_TOPIC = os.getenv("KAFKA_SCORING_TOPIC", "scoring")

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

conn = psycopg2.connect(
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    dbname=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD
)
cur = conn.cursor()

cur.execute("""
    CREATE TABLE IF NOT EXISTS scoring_results (
        transaction_id TEXT PRIMARY KEY,
        score FLOAT,
        fraud_flag BOOLEAN
    )
""")
conn.commit()

consumer = Consumer({
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': 'score-writer-group',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe([SCORING_TOPIC])

print("Listening for messages...")
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Kafka error: {msg.error()}")
            continue

        records = json.loads(msg.value().decode('utf-8'))
        for record in records:
            cur.execute("""
                INSERT INTO scoring_results (transaction_id, score, fraud_flag)
                VALUES (%s, %s, %s)
                ON CONFLICT (transaction_id) DO NOTHING
            """, (record['transaction_id'], record['score'], bool(record['fraud_flag'])))
            conn.commit()
except KeyboardInterrupt:
    print("Stopping service...")
finally:
    consumer.close()
    cur.close()
    conn.close()

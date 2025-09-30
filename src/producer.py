import json
import os
import time
from typing import Optional

from confluent_kafka import Message, Producer
from faker import Faker
from loguru import logger

# giving some time to kafka
logger.info("Producer Starting")
time.sleep(30)

fake = Faker()

conf = {"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092")}
producer = Producer(conf)


def generate_data() -> dict:
    return {
        "id": fake.uuid4(),
        "timestamp": fake.date_time_this_year().isoformat(),
        "amount": round(fake.pyfloat(left_digits=3, right_digits=2, positive=True), 2),
        "currency": "USD",
        "sender": fake.name(),
        "receiver": fake.name(),
    }


def delivery_report(err: Optional[Exception], msg: Message) -> None:
    if err is not None:
        logger.warning(f"Delivery failed! {err}")
    else:
        logger.info(f"Delivered to {msg.topic()} [{msg.partition()}]")


while True:
    transaction = generate_data()
    producer.produce(
        topic="transactions",
        value=json.dumps(transaction),
        callback=delivery_report,
    )
    producer.poll(0)
    time.sleep(2)

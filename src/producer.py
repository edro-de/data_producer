import json
import os
import sys
import time
from typing import Optional

from confluent_kafka import Message, Producer
from faker import Faker
from loguru import logger

# giving some time to kafka
time.sleep(30)
fake = Faker()
interval = 0.1

logger.configure(handlers=[{"sink": sys.stdout, "level": os.getenv("LOG_LEVEL", "INFO")}])


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
        logger.debug(f"Delivered to {msg.topic()} [{msg.partition()}]")


if __name__ == "__main__":
    conf = {"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka1:9092")}
    producer = Producer(conf)

    while True:
        transaction = generate_data()
        producer.produce(
            topic="transactions",
            key=os.getenv("PROD_KEY", f"producer_{int(time.time())}"),
            value=json.dumps(transaction),
            callback=delivery_report,
        )
        producer.poll(0)
        time.sleep(interval)

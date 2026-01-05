# consumer/testconsumer.py
import os
import socket
import time

from engine.kafka.base_consumer import BaseConsumer


class TestConsumer(BaseConsumer):
    def handle_message(self, value, key, message):
        print(
            f"[TEST] topic={message.topic} "
            f"partition={message.partition} "
            f"offset={message.offset} "
            f"key={key} value={value}"
        )
        # 테스트용 처리
        time.sleep(1)


def _build_client_id(base, worker_id):
    host = socket.gethostname()
    pid = os.getpid()
    return f"{base}-{host}-w{worker_id}-p{pid}"


if __name__ == "__main__":
    topic = "test-topic"
    group_id = "test-group"
    dlq_topic = "test-dlq"
    worker_id = 0

    client_id = os.environ.get("KAFKA_CLIENT_ID")
    if not client_id:
        client_id = _build_client_id("test-consumer", worker_id)

    consumer = TestConsumer(
        topic=topic,
        group_id=group_id,
        client_id=client_id,
        dlq_topic=dlq_topic,
    )
    consumer.start()

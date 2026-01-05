import json
from pathlib import Path
import argparse
import os

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaError


class TopicInitializer:
    def __init__(self, bootstrap_servers):
        self.admin = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id="topic-initializer"
        )

    def close(self):
        self.admin.close()

    def initialize_from_dir(self, topic_dir):
        topic_dir = Path(topic_dir)
        if not topic_dir.exists() or not topic_dir.is_dir():
            raise FileNotFoundError(f"Topic directory not found: {topic_dir}")

        json_files = sorted(topic_dir.glob("*.json"))
        if not json_files:
            raise ValueError(f"No json files found in {topic_dir}")

        for json_path in json_files:
            self._initialize_from_file(json_path)

    def _initialize_from_file(self, json_path):
        if not json_path.exists():
            raise FileNotFoundError(f"Topic json not found: {json_path}")

        with json_path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        topics = data.get("topics")
        if not isinstance(topics, list):
            raise ValueError(f"{json_path}: JSON must contain 'topics': []")

        for topic in topics:
            self._create_main_topic(topic)
            self._create_dlq_topic(topic)

    def _create_main_topic(self, topic):
        name = topic["name"]
        partitions = int(topic.get("partitions", 3))
        rf = int(topic.get("replication_factor", 1))
        configs = topic.get("configs")

        self._create_topic(
            name=name,
            partitions=partitions,
            replication_factor=rf,
            configs=configs
        )

    def _create_dlq_topic(self, topic):
        dlq = topic.get("dlq")
        if not isinstance(dlq, dict) or not dlq.get("enabled", False):
            return

        name = f"{topic['name']}.DLQ"
        partitions = int(dlq.get("partitions", 1))
        rf = int(topic.get("replication_factor", 1))
        configs = dlq.get("configs")

        self._create_topic(
            name=name,
            partitions=partitions,
            replication_factor=rf,
            configs=configs
        )

    def _create_topic(self, name, partitions, replication_factor, configs=None):
        # kafka topic configs는 보통 문자열 값이 안전함
        if isinstance(configs, dict):
            configs = {str(k): str(v) for k, v in configs.items()}

        try:
            self.admin.create_topics(
                [NewTopic(
                    name=name,
                    num_partitions=partitions,
                    replication_factor=replication_factor,
                    topic_configs=configs
                )]
            )
        except TopicAlreadyExistsError:
            pass
        except KafkaError as e:
            raise RuntimeError(f"Failed to create topic {name}: {e}") from e


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka topic initializer")
    parser.add_argument(
        "--bootstrap",
        default=os.environ.get("KAFKA_URL", "kafka:9092"),
        help="Kafka bootstrap servers (default: env KAFKA_URL or kafka:9092)"
    )
    parser.add_argument(
        "--topic-dir",
        default=os.environ.get("TOPIC_DIR", "./topics"),
        help="Directory containing topic json files"
    )

    args = parser.parse_args()

    initializer = TopicInitializer(args.bootstrap)
    try:
        initializer.initialize_from_dir(args.topic_dir)
    finally:
        initializer.close()

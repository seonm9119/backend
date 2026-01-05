import json
import signal
import time
import traceback

from abc import ABC, abstractmethod

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from kafka.consumer.subscription_state import ConsumerRebalanceListener

from engine.kafka.config import *


class _RebalanceListener(ConsumerRebalanceListener):
    def __init__(self, owner):
        self.owner = owner

    def on_partitions_revoked(self, revoked):
        # 리밸런스 직전 dirty commit으로 중복 최소화
        try:
            self.owner._commit_dirty_now_safely(reason="rebalance_revoke")
        except Exception as e:
            pass

    def on_partitions_assigned(self, assigned):
        # 필수 구현(추상 메서드). 지금은 특별히 할 일 없으면 패스.
        pass

class BaseConsumer(ABC):

    def __init__(self, topic, group_id, client_id, dlq_topic=None):
        self.topic = topic
        self.dlq_topic = dlq_topic
        self.group_id = group_id
        self.client_id = client_id

        self.consumer = None
        self.producer = None

        self._processed_since_commit = 0
        self._last_commit_time = time.time()

        self._stopping = False
        self._install_signal_handlers()


    @staticmethod
    def _json_deserializer(b):
        if b is None:
            return None
        try:
            return json.loads(b.decode("utf-8"))
        except Exception as e:
            raise ValueError(f"Invalid JSON message: {b!r}") from e

    @staticmethod
    def _json_serializer(v):
        return json.dumps(v, ensure_ascii=False).encode("utf-8")


    def stop(self):
        """
        stop 플래그 + poll 탈출(wakeup)
        """
        self._stopping = True
        try:
            if self.consumer is not None:
                self.consumer.wakeup()
        except Exception:
            pass

    def close(self):
        # producer
        try:
            if self.producer is not None:
                try:
                    self.producer.flush(timeout=PRODUCER_FLUSH_TIMEOUT_SEC)
                finally:
                    self.producer.close()
        except Exception:
            pass
        finally:
            self.producer = None

        # consumer
        try:
            if self.consumer is not None:
                self.consumer.close()
        except Exception:
            pass
        finally:
            self.consumer = None

    def _install_signal_handlers(self):
        def _handler(signum, frame):
            self.stop()

        signal.signal(signal.SIGINT, _handler)
        signal.signal(signal.SIGTERM, _handler)

    @abstractmethod
    def handle_message(self, value, key, message):
        raise NotImplementedError

    def _ensure_consumer(self):
        if self.consumer is not None:
            return

        # subscribe + rebalance listener 쓰려면 topic을 생성자에 바로 넣기보다 subscribe를 사용
        self.consumer = KafkaConsumer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            group_id=self.group_id,
            client_id=self.client_id,
            enable_auto_commit=ENABLE_AUTO_COMMIT,
            auto_offset_reset=AUTO_OFFSET_RESET,
            max_poll_records=MAX_POLL_RECORDS,
            value_deserializer=self._json_deserializer,
            key_deserializer=self._json_deserializer,

            session_timeout_ms=SESSION_TIMEOUT_MS,
            heartbeat_interval_ms=HEARTBEAT_INTERVAL_MS,
            max_poll_interval_ms=MAX_POLL_INTERVAL_MS,
            request_timeout_ms=REQUEST_TIMEOUT_MS,
        )

        self.consumer.subscribe([self.topic], listener=_RebalanceListener(self))

    def _ensure_producer(self):
        if self.producer is not None:
            return

        self.producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=self._json_serializer,
            key_serializer=self._json_serializer,
            acks=PRODUCER_ACKS,
            linger_ms=PRODUCER_LINGER_MS,
            retries=PRODUCER_RETRIES,
            request_timeout_ms=PRODUCER_REQUEST_TIMEOUT_MS,
        )

    def start(self):
        self._ensure_consumer()

        try:
            while not self._stopping:
                try:
                    records = self.consumer.poll(
                        timeout_ms=POLL_TIMEOUT_MS,
                        max_records=MAX_POLL_RECORDS
                    )
                except Exception as e:
                    # wakeup()로 인해 poll이 예외로 풀릴 수 있음
                    if self._stopping:
                        break
                    print(f"[ERROR] poll failed: {e}")
                    time.sleep(POLL_ERROR_BACKOFF_SEC)
                    continue

                if not records:
                    self._commit_if_needed()
                    continue

                for tp, msgs in records.items():
                    for msg in msgs:
                        if self._stopping:
                            break
                        self._handle_one(msg)

                self._commit_if_needed()

        finally:
            if COMMIT_ON_STOP:
                self._commit_dirty_now_safely(reason="stop")
            self.close()


    def _handle_one(self, msg):
        try:
            self.handle_message(msg.value, msg.key, msg)
            self._processed_since_commit += 1

        except Exception as e:
            handled = self.on_process_error(e, msg)
            if handled:
                # DLQ 전송 성공이 보장된 경우만 처리완료로 간주
                self._processed_since_commit += 1
            else:
                self._stopping = True
                raise

    def on_process_error(self, exc, msg):
        if ENABLE_DLQ:
            self._send_to_dlq(exc, msg)  # 여기서 ACK 확인
            return True
        return False

    def _commit_if_needed(self):
        if ENABLE_AUTO_COMMIT or self.consumer is None:
            return

        if self._processed_since_commit <= 0:
            return

        now = time.time()

        if self._processed_since_commit >= COMMIT_EVERY_N:
            self._commit_with_retry(reason="count_threshold")
            return

        if (now - self._last_commit_time) >= COMMIT_INTERVAL_SEC:
            self._commit_with_retry(reason="time_threshold")

    def _commit_dirty_now_safely(self, reason="manual"):
        if ENABLE_AUTO_COMMIT or self.consumer is None:
            return
        if self._processed_since_commit <= 0:
            return
        self._commit_with_retry(reason=reason)

    def _commit_with_retry(self, reason="unknown"):
        if self.consumer is None or ENABLE_AUTO_COMMIT:
            return

        for i in range(COMMIT_RETRIES + 1):
            try:
                self.consumer.commit()
                self._processed_since_commit = 0
                self._last_commit_time = time.time()
                return
            except KafkaError as e:
                if i >= COMMIT_RETRIES:
                    print(f"[ERROR] commit failed final (reason={reason}): {e}")
                    raise

                sleep_sec = COMMIT_BACKOFF_BASE_SEC * (2 ** i)
                if sleep_sec > COMMIT_BACKOFF_MAX_SEC:
                    sleep_sec = COMMIT_BACKOFF_MAX_SEC

                print(
                    f"[WARN] commit failed (reason={reason}) "
                    f"retry={i}/{COMMIT_RETRIES} sleep={sleep_sec}s: {e}"
                )
                time.sleep(sleep_sec)

    def _send_to_dlq(self, exc, msg):
        if not ENABLE_DLQ or self.dlq_topic is None:
            return

        self._ensure_producer()

        payload = {
            "source_topic": msg.topic,
            "source_partition": msg.partition,
            "source_offset": msg.offset,
            "timestamp": getattr(msg, "timestamp", None),
            "key": msg.key,
            "value": msg.value,
            "error": {
                "type": type(exc).__name__,
                "message": str(exc),
                "traceback": traceback.format_exc(),
            },
        }

        future = self.producer.send(self.dlq_topic, key=msg.key, value=payload)
        future.get(timeout=DLQ_SEND_TIMEOUT_SEC)

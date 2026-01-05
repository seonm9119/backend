# engine/kafka/config.py

# =========================
# Kafka Consumer
# =========================
import os
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092").split(",")

# auto commit을 켜면 BaseConsumer의 수동 커밋 로직은 동작 안 함
ENABLE_AUTO_COMMIT = False
AUTO_OFFSET_RESET = "earliest"   # "latest" 도 가능

# poll
POLL_TIMEOUT_MS = 1000
MAX_POLL_RECORDS = 1             # 처리 최대 10분이면 작게(1~10) 추천

# long processing(최대 10분) 대응
SESSION_TIMEOUT_MS = 15000       # 15s
HEARTBEAT_INTERVAL_MS = 3000     # 3s (보통 session의 1/3)
MAX_POLL_INTERVAL_MS = 1800000   # 30분(10분의 3배 여유)
REQUEST_TIMEOUT_MS = 305000      # 보통 max_poll_interval_ms보다 약간 크게

# =========================
# Commit policy
# =========================
COMMIT_EVERY_N = 1               # N개 처리마다 커밋
COMMIT_INTERVAL_SEC = 30         # 또는 시간 기준 커밋(초)
COMMIT_ON_STOP = True            # 종료 시 dirty 커밋

# commit retry/backoff
COMMIT_RETRIES = 5
COMMIT_BACKOFF_BASE_SEC = 0.5
COMMIT_BACKOFF_MAX_SEC = 5.0

# =========================
# DLQ
# =========================
ENABLE_DLQ = True
DLQ_SEND_TIMEOUT_SEC = 10

# =========================
# Kafka Producer (DLQ)
# =========================
PRODUCER_ACKS = "all"            # DLQ 안정성: "all" 추천
PRODUCER_LINGER_MS = 5
PRODUCER_RETRIES = 3
PRODUCER_REQUEST_TIMEOUT_MS = 30000
PRODUCER_FLUSH_TIMEOUT_SEC = 5

# =========================
# Poll error handling
# =========================
POLL_ERROR_BACKOFF_SEC = 1.0

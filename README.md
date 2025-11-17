## ⚙️ Infrastructure Stack

- Docker images: `postgres:15`, `apache/kafka:3.7.0` (KRaft), `provectuslabs/kafka-ui:v0.7.2`
- Compose files:
  - `docker-compose.infra.yml`: Postgres + Kafka + Kafka UI
  - `docker-compose.yml`: Flask app container
- ⚠️ 필요한 환경 변수는 `example.env` 참고

### 🔌 Bring up infra only
```
cp example.env .env   # or customize your own .env
docker compose -f docker-compose.infra.yml up -d
```
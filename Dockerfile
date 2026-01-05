FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    supervisor \
    procps \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /var/run

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

COPY consumers.conf /etc/supervisor/conf.d/consumers.conf

CMD ["supervisord", "-c", "/etc/supervisor/conf.d/consumers.conf"]

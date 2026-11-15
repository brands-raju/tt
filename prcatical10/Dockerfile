
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .
COPY consumer.py .

CMD ["python", "-u", "producer.py"]
# ractical 10 – Concept (Short)
# Aim: Real-Time Air Quality Monitoring using Kafka, ClickHouse and Grafana.

# This practical collects simulated air quality data and displays it in a dashboard.

# Working:
# 1. Producer: Generates air quality data (PM2.5, PM10, temperature and city).
# 2. Kafka: Transfers data in real time.
# 3. Consumer: Reads data from Kafka.
# 4. ClickHouse: Stores the received data.
# 5. Grafana: Displays data using tables and graphs
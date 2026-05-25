FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY analyzer.py .

CMD ["python", "analyzer.py", "--nats", "nats://nats:4222"]

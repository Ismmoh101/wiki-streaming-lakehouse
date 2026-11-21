FROM python:3.12-slim
WORKDIR /app
COPY requirements-producer.txt .
RUN pip install --no-cache-dir -r requirements-producer.txt
COPY producer/ producer/
CMD ["python", "-m", "producer.producer"]

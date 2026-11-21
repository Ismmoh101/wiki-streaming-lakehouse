FROM apache/spark:4.0.1-python3
USER root
RUN pip install --no-cache-dir pymongo==4.10.1
# Pre-download Kafka, Iceberg and AWS jars at build time so the job starts fast.
ENV PACKAGES=org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0,org.apache.iceberg:iceberg-aws-bundle:1.10.0
RUN echo "pass" > /tmp/noop.py && \
    /opt/spark/bin/spark-submit --conf spark.jars.ivy=/opt/ivy --packages $PACKAGES /tmp/noop.py && \
    chmod -R a+rwX /opt/ivy
WORKDIR /app
COPY streaming/ streaming/
COPY docker/run_stream.py .
USER spark

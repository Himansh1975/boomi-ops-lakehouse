# bookworm: Spark 3.5 supports Java 17, and newer Debian releases no longer package it.
FROM python:3.11-slim-bookworm

# Spark needs a JVM.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/project
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Download the Delta Lake jars at build time so runs work offline.
RUN python -c "from delta import configure_spark_with_delta_pip; from pyspark.sql import SparkSession; \
configure_spark_with_delta_pip(SparkSession.builder.master('local[1]')).getOrCreate().stop()"

ENV PYTHONPATH=/opt/project/src

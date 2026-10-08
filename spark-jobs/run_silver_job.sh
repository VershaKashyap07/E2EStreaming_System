#!/bin/bash
# Run from your host: bash spark-jobs/run_silver_job.sh
# This execs into the running 'spark' container and submits the job.

docker exec spark spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5 \
  --master local[*] \
  /home/iceberg/spark-jobs/silver_job.py

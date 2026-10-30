#!/bin/sh
# Runs when LocalStack is ready (mounted into /etc/localstack/init/ready.d by docker-compose.yml).
set -e

awslocal s3 mb s3://checkout-lake-raw || true
awslocal s3 mb s3://checkout-checkpoints || true

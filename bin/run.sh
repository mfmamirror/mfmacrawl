#!/usr/bin/env bash

set -euf -o pipefail

TIMESTAMP=$(date +%Y-%m-%d)

trickle -d 200 \
        -u 200 \
        scrapy crawl mfma \
            -t jsonlines \
            -s S3_BUCKET_NAME=$S3_BUCKET_NAME \
            -s AWS_KEY_ID=$AWS_KEY_ID \
            -s AWS_KEY_SECRET=$AWS_KEY_SECRET \
            -s INTERNET_ARCHIVE_KEY_ID=$INTERNET_ARCHIVE_KEY_ID \
            -s INTERNET_ARCHIVE_KEY_SECRET=$INTERNET_ARCHIVE_KEY_SECRET \
            --loglevel=INFO \
            --logfile=/var/log/mfmacrawl/mfmacrawl-${TIMESTAMP}.log \
            -o /var/lib/mfmacrawl/mfmacrawl-${TIMESTAMP}.jsonlines

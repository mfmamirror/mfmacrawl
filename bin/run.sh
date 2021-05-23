#!/usr/bin/env bash

set -euf -o pipefail

TIMESTAMP=$(date +%Y-%m-%dT%H%M%S)

trickle -s \
        -d 200 \
        -u 200 \
        scrapy crawl mfma \
            --set S3_BUCKET_NAME=$S3_BUCKET_NAME \
            --set AWS_KEY_ID=$AWS_KEY_ID \
            --set AWS_KEY_SECRET=$AWS_KEY_SECRET \
            --set INTERNET_ARCHIVE_KEY_ID=$INTERNET_ARCHIVE_KEY_ID \
            --set INTERNET_ARCHIVE_KEY_SECRET=$INTERNET_ARCHIVE_KEY_SECRET \
            --loglevel=INFO \
            --logfile=/var/log/mfmacrawl/mfmacrawl-${TIMESTAMP}.log \
            --output file:///var/lib/mfmacrawl/mfmacrawl-${TIMESTAMP}.jsonlines:jsonlines \
            $@

# Scrape mfma.treasury.gov.za and build mfmamirror.github.io jekyl source files

## Spider Arguments

- `scrape_menu` - optional - whether the menu should be scraped for data items and the links crawled futher
- `start_url` - optional - a single replacement for the default start URL of the site root. Default `true`

## Project Settings

- `S3_BUCKET_NAME`
- `AWS_KEY_ID`
- `AWS_KEY_SECRET`
- `ITEM_PIPELINES`: {"mfma.pipelines.DepagingPipeline": 100,"mfma.pipelines.InternetArchiveFileArchivePipeline": 100}

## Set up dev environment

    poetry install

Each time you run python code, you need to enter the python virtual environment, e.g.

    poetry shell

## Run tests

    poetry run pytest

## run scraper locally

    poetry run scrapy crawl mfma -o mfma.json

Scrape a specific URL at a specific depth for debugging or something

    poetry run scrapy crawl mfma -s DEPTH_LIMIT=1 -a scrape_menu=false -a start_url=http://mfma.treasury.gov.za/Circulars/Pages/default.aspx

To run without pipelines:

    poetry run scrapy shell -s 'ITEM_PIPELINES={}'

## Run monthly

Install into cron

    44 20 1 * * cd /home/pi/mfmamirror/mfmacrawl/ && trickle -d 200 -u 200 /home/pi/mfmamirror/mfmacrawl/env/bin/scrapy crawl mfma -t jsonlines -s S3_BUCKET_NAME=mfmamirror -s AWS_KEY_ID=... -s AWS_KEY_SECRET=... -s INTERNET_ARCHIVE_KEY_ID=... -s INTERNET_ARCHIVE_KEY_SECRET=... --loglevel=INFO --logfile=/home/pi/mfmamirror/mfma.log-$(date +\%Y-\%m-\%d) -o /home/pi/mfmamirror/mfma.jsonlines-$(date +\%Y-\%m-\%d) 2>&1 >> /home/pi/mfmamirror/cron.log

## running within docker

    docker build -t mfmacrawl .
    docker run --rm --env-file .env -v /var/log/mfmacrawl:/var/log/mfmacrawl -v /var/lib/mfmacrawl:/var/lib/mfmacrawl -v /var/lib/project-data-dir:/app/.scrapy mfmacrawl

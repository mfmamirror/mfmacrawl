# Scrape mfma.treasury.gov.za and build mfmamirror.github.io jekyl source files

## run scraper locally

    scrapy crawl mfma -O mfma.json

## build mfmamirror.github.io sources locally

    python buildmirror.py mfma.json ../mfmamirror
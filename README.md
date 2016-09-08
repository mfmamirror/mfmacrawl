# Scrape mfma.treasury.gov.za and build mfmamirror.github.io jekyl source files

## Spider Arguments

- `scrape_menu` - optional - whether the menu should be scraped for data items and the links crawled futher
- `start_url` - optional - a single replacement for the default start URL of the site root. Default `true`

## run scraper locally

    scrapy crawl mfma -o mfma.json

Scrape a specific URL at a specific depth for debugging or something

    scrapy crawl mfma -o mfma.json -s DEPTH_LIMIT=1 -a scrape_menu=false -a start_url=http://mfma.treasury.gov.za/Circulars/Pages/default.aspx

## build mfmamirror.github.io sources locally

    python buildmirror.py mfma.json ../mfmamirror
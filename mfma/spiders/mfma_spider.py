import scrapy
from mfma.items import PageItem


class MfmaSpider(scrapy.Spider):
    name = "mfma"
    allowed_domains = ["mfma.treasury.gov.za"]
    start_urls = [
        "http://mfma.treasury.gov.za"
    ]

    def parse(self, response):
        filename = response.url.split("/")[-2] + '.html'
        path = 'scrape/' + filename
        with open(path, 'wb') as f:
            f.write(response.body)

        item = PageItem()
        item['body'] = response.selector.css('.ms-WPBody').extract()
        # response.selector.xpath("//div[@class='ms-WPBody']")
        item['menu'] = response.selector.css('#zz1_QuickLaunchMenu').extract()
        item['breadcrumbs'] = response.selector.css('#ctl00_PlaceHolderTitleBreadcrumb_siteMapPath').extract()
        item['title'] = response.selector.css('.breadcrumbCurrent').extract()
        yield item

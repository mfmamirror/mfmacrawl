import scrapy
from mfma.items import PageItem, MenuItem


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

        menu_item = MenuItem()
        menu_items = []
        for menu_link in response.selector.css('#zz1_QuickLaunchMenu a'):
            menu_items.append({
                'href': menu_link.xpath('@href').extract()[0],
                'text': menu_link.xpath('text()').extract()[0],
            })
        menu_item['menu_items'] = menu_items
        yield menu_item

        item = PageItem()
        item['body'] = response.selector.css('.ms-WPBody').extract()
        # response.selector.xpath("//div[@class='ms-WPBody']")


        item['breadcrumbs'] = response.selector.css('#ctl00_PlaceHolderTitleBreadcrumb_siteMapPath').extract()
        item['title'] = response.selector.css('.breadcrumbCurrent').extract()
        yield item

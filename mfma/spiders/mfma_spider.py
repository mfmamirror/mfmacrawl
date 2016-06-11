import scrapy
from mfma.items import PageItem, MenuItem
import urlparse


class MfmaSpider(scrapy.Spider):
    name = "mfma"
    allowed_domains = ["mfma.treasury.gov.za"]
    start_urls = [
        "http://mfma.treasury.gov.za"
    ]

    def parse(self, response):
        for result in self.parse_home(response):
            yield result

    def parse_home(self, response):
        filename = response.url.split("/")[-2] + '.html'
        path = 'scrape/' + filename
        with open(path, 'wb') as f:
            f.write(response.body)

        menu_item = MenuItem()
        menu_items = []
        for menu_link in response.selector.css('#zz1_QuickLaunchMenu a'):
            url = menu_link.xpath('@href').extract()[0]
            if urlparse.urlparse(url).scheme:
                abs_url = url
            else:
                abs_url = urlparse.urljoin(response.url, url)
            yield scrapy.Request(abs_url, callback=self.parse_page)
            menu_items.append({
                'url': url,
                'text': menu_link.xpath('text()').extract()[0],
            })
        menu_item['menu_items'] = menu_items
        yield menu_item

        item = PageItem()
        item['body'] = response.selector.css('.ms-WPBody > P').extract()
        breadcrumbs_css = '#ctl00_PlaceHolderTitleBreadcrumb_siteMapPath'
        item['breadcrumbs'] = response.selector.css(breadcrumbs_css).extract()
        title_css = '.breadcrumbCurrent'
        item['title'] = response.selector.css(title_css).xpath('text()').extract()
        yield item

    def parse_page(self, response):
        pass
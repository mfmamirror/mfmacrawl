import scrapy
from mfma.items import PageItem, MenuItem
import urlparse
from scrapy.linkextractors.lxmlhtml import LxmlLinkExtractor


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
        menu_item['type'] = 'menu'
        menu_items = []
        for menu_link in response.selector.css('#zz1_QuickLaunchMenu a'):
            url = menu_link.xpath('@href').extract()[0]
            menu_items.append({
                'url': self.dedotnet(url),
                'text': menu_link.xpath('text()').extract()[0],
            })

            if urlparse.urlparse(url).scheme:
                abs_url = url
            else:
                abs_url = urlparse.urljoin(response.url, url)
            yield scrapy.Request(abs_url, callback=self.parse_page)
        menu_item['menu_items'] = menu_items
        yield menu_item
        yield self.page_item(response)

    def page_item(self, response):
        item = PageItem()
        item['type'] = 'page'
        url = urlparse.urlparse(response.url)
        item['original_url'] = response.url
        item['path'] = self.dedotnet(url.path)
        if response.selector.css('.ms-WPBody > P'):
            item['body'] = response.selector.css('.ms-WPBody > P')[0].extract()
        breadcrumbs_css = '#ctl00_PlaceHolderTitleBreadcrumb_siteMapPath'
        if response.selector.css(breadcrumbs_css):
            item['breadcrumbs'] = response.selector.css(breadcrumbs_css)[0].extract()
        title_css = '.breadcrumbCurrent'
        if response.selector.css(title_css):
            item['title'] = response.selector.css(title_css).xpath('text()')[0].extract()
        return item

    def parse_page(self, response):
        print
        print
        pagelinkextractor = LxmlLinkExtractor()
        for link in pagelinkextractor.extract_links(response):
            url = link.url
            if self.is_forms_url(link.url):
                url = self.fix_forms_url(link.url)
            if url.endswith('txt') \
               or url.endswith('db') \
               or url.endswith('xml') \
               or 'Authenticate' in url:
                print '  SKIPPING  ' + url
                continue
            print url
            # yield scrapy.Request(url, callback=self.parse_page)
        yield self.page_item(response)

    def is_forms_url(self, url):
        parsed = urlparse.urlparse(url)
        return 'RootFolder' in url

    def fix_forms_url(self, url):
        parsed_url = urlparse.urlparse(url)
        parsed_qs = urlparse.parse_qs(parsed_url.query)
        if 'RootFolder' in parsed_qs:
            return "%s://%s%s" % (parsed_url.scheme,
                                  parsed_url.netloc,
                                  parsed_qs['RootFolder'][0])
        else:
            return url

    def dedotnet(self, path):
        path = path.replace('/Pages/Default.aspx', 'index.html')
        path = path.replace('/Pages/default.aspx', 'index.html')
        path = path.replace('.aspx', '/index.html')
        return path

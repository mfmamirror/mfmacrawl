import scrapy
from mfma.items import PageItem, MenuItem
import urlparse
from scrapy.linkextractors.lxmlhtml import LxmlLinkExtractor
from bs4 import BeautifulSoup
import re


class MfmaSpider(scrapy.Spider):
    name = "mfma"
    allowed_domains = ["mfma.treasury.gov.za"]
    start_urls = [
        "http://mfma.treasury.gov.za"
    ]

    def __init__(self):
        self.base = "http://mfma.treasury.gov.za"

        self.form_table_css = 'div.mainContent > table > tr > td#MSOZoneCell_WebPartWPQ2'
        self.simple_content_css = '.mainContent'

    def parse(self, response):
        for result in self.parse_home(response):
            yield result

    def parse_home(self, response):
        menu_item = MenuItem()
        menu_item['type'] = 'menu'
        menu_items = []
        for menu_link in response.selector.css('#zz1_QuickLaunchMenu a'):
            url = menu_link.xpath('@href').extract()[0]
            menu_items.append({
                'url': self.dedotnet(url, indexhtml=False),
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

        if response.selector.css(self.form_table_css):
            self.set_form_table_content(item, response)
        elif response.selector.css(self.simple_content_css):
            self.set_simple_content(item, response)

        title_css = '.breadcrumbCurrent'
        if response.selector.css(title_css):
            item['title'] = response.selector.css(title_css).xpath('text()')[0].extract()
        return item

    def set_simple_content(self, item, response):
        body = self.fix_links(response.selector.css(self.simple_content_css)[0].extract())
        body = self.clean_html(body)
        item['body'] = body

        breadcrumbs_css = '#ctl00_PlaceHolderTitleBreadcrumb_siteMapPath'
        css_match = response.selector.css(breadcrumbs_css)
        if css_match:
            item['breadcrumbs'] = self.breadcrumbs_html(css_match)

    def set_form_table_content(self, item, response):
        item['body'] = "fancy table"

        breadcrumbs_css = '#ctl00_PlaceHolderTitleBreadcrumb_ContentMap'
        css_match = response.selector.css(breadcrumbs_css)
        if css_match:
            item['breadcrumbs'] = self.breadcrumbs_html(css_match)

    def breadcrumbs_html(self, match):
        breadcrumbs_html = match[0].extract()
        soup = BeautifulSoup(breadcrumbs_html, "html.parser")
        for a in soup.findAll('a'):
            a['href'] = self.dedotnet(a['href'], indexhtml=False)
        return str(soup)

    def parse_page(self, response):
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
            # yield scrapy.Request(url, callback=self.parse_page)
        yield self.page_item(response)

    def fix_links(self, html):
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.findAll('a'):
            try:
                url = a['href']
                purl = urlparse.urlparse(url)
                if not purl.hostname:
                    url = self.base + url
                if self.is_forms_url(url):
                    url = self.fix_forms_url(url)
                a['href'] = url
            except KeyError:
                import pdb; pdb.set_trace

        return str(soup)

    def clean_html(self, html):
        soup = BeautifulSoup(html, "html.parser")
        whitelist = {'src', 'href', 'target', 'alt'}
        for a in soup.findAll('a'):
            for tag in soup.findAll(True):
                for a in tag.attrs.keys():
                    if a not in whitelist:
                        del tag.attrs[a]
        html = str(soup)
        html = re.sub(r'</?br>\s*</?br>(\s*</?br>)*', '<br><br>', html)
        return html

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

    def dedotnet(self, path, indexhtml=True):
        if indexhtml:
            replacement = '/index.html'
        else:
            replacement = '/'
        path = path.replace('/Pages/Default.aspx', replacement)
        path = path.replace('/Pages/default.aspx', replacement)
        path = path.replace('.aspx', replacement)
        return path

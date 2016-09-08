import scrapy
from mfma.items import PageItem, MenuItem, TableFormItem
import urlparse
import urllib
from scrapy.linkextractors.lxmlhtml import LxmlLinkExtractor
from bs4 import BeautifulSoup
import re
import os


class MfmaSpider(scrapy.Spider):
    name = "mfma"
    allowed_domains = ["mfma.treasury.gov.za"]
    start_urls = [
        "http://mfma.treasury.gov.za"
    ]

    def __init__(self, start_url=None, scrape_menu="true"):
        self.base = "http://mfma.treasury.gov.za"

        self.form_table_css = 'div.mainContent > table > tr > td#MSOZoneCell_WebPartWPQ2'
        self.simple_content_css = '.mainContent'

        if start_url:
            self.start_urls = [start_url]

        self.should_scrape_menu = scrape_menu == 'true'

    def scrape_menu(self, response):
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
            yield scrapy.Request(abs_url)
        menu_item['menu_items'] = menu_items
        yield menu_item
        for item in self.page_item(response):
            yield item

    def page_item(self, response):
        page_item = PageItem()
        page_item['type'] = 'page'

        if response.selector.css(self.form_table_css):
            for item in self.set_form_table_content(page_item, response):
                yield item
        elif response.selector.css(self.simple_content_css):
            self.set_simple_content(page_item, response)

        title_css = '.breadcrumbCurrent'
        if response.selector.css(title_css):
            page_item['title'] = response.selector.css(title_css).xpath('text()')[0].extract()
        yield page_item

    def set_simple_content(self, page_item, response):
        url = urlparse.urlparse(response.url)
        page_item['original_url'] = response.url
        page_item['path'] = self.dedotnet(url.path)
        body = self.fix_links(response.selector.css(self.simple_content_css)[0].extract())
        body = self.clean_html(body)
        page_item['body'] = body

        breadcrumbs_css = '#ctl00_PlaceHolderTitleBreadcrumb_siteMapPath'
        css_match = response.selector.css(breadcrumbs_css)
        if css_match:
            page_item['breadcrumbs'] = self.breadcrumbs_html(css_match)

    def set_form_table_content(self, page_item, response):
        label_table_xpath = 'td[@class="ms-vb-title"]/table[@class="ms-unselectedtitle"]'
        row_xpath = '//' + label_table_xpath + '/../..'
        rows = response.xpath(row_xpath)
        url = response.url
        if self.is_forms_url(url):
            url = self.fix_forms_url(url)
        purl = urlparse.urlparse(url)
        location = self.dedotnet(purl.path, indexhtml=False)
        page_item['original_url'] = url
        page_item['path'] = location + '/index.html'

        for row in rows:
            label_xpath = label_table_xpath + '/tr/td/a/text()'
            label = row.xpath(label_xpath)[0].extract()
            url_xpath = label_table_xpath + '/@url'
            row_url = row.xpath(url_xpath)[0].extract()
            path = urlparse.urlparse(row_url).path
            user_xpath = 'td[@class="ms-vb-user"]/text()'
            user = row.xpath(user_xpath)[0].extract()
            mod_date_xpath = 'td[@class="ms-vb2"]/nobr/text()'
            mod_date = row.xpath(mod_date_xpath)[0].extract()
            row_item = TableFormItem()
            row_item['type'] = 'table_form_item'
            row_item['label'] = label
            row_item['path'] = path
            row_item['modified_date'] = mod_date
            row_item['user'] = user
            row_item['location'] = location
            yield row_item

            regex = '^.+(\..{1,4})$'
            if not re.match(regex, path):
                child =  "http://%s%s" % (purl.netloc, path)
                yield scrapy.Request(child)

        nextlink = response.xpath('//img[@alt="Next"]')
        if nextlink:
            qs = urllib.urlencode({'p_FileLeafRef': label, 'Paged': 'TRUE'})
            next_page_url = urlparse.urljoin(url, '?' + qs)
            yield scrapy.Request(next_page_url)


        breadcrumbs_css = '#ctl00_PlaceHolderTitleBreadcrumb_ContentMap'
        css_match = response.selector.css(breadcrumbs_css)
        if css_match:
            page_item['breadcrumbs'] = self.breadcrumbs_html(css_match)
        raise StopIteration

    def breadcrumbs_html(self, match):
        breadcrumbs_html = match[0].extract()
        breadcrumbs_html = self.fix_links(breadcrumbs_html)
        soup = BeautifulSoup(breadcrumbs_html, "html.parser")
        for a in soup.findAll('a'):
            a['href'] = self.dedotnet(a['href'], indexhtml=False)
        return str(soup)

    def parse(self, response):
        if self.should_scrape_menu:
            self.should_scrape_menu = False
            for item in self.scrape_menu(response):
                yield item

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
            # yield scrapy.Request(url)
        for item in self.page_item(response):
            yield item

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
            parsed_rootfolder = urlparse.urlparse(parsed_qs['RootFolder'][0])
            return "http://%s%s" % (parsed_url.netloc,
                                    parsed_rootfolder.path)
        else:
            return url

    def dedotnet(self, path, indexhtml=True, trailing_slash=True):
        if indexhtml:
            replacement = '/index.html'
        else:
            replacement = '/' if trailing_slash else ''
        path = path.replace('/Pages/Default.aspx', replacement)
        path = path.replace('/Pages/default.aspx', replacement)
        path = path.replace('/Forms/AllItems.aspx', replacement)
        path = path.replace('.aspx', replacement)
        return path

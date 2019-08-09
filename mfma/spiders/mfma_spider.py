from bs4 import BeautifulSoup
from mfma.items import PageItem, MenuItem, FileItem
import logging
import re
import scrapy
import urllib
import urlparse

logger = logging.getLogger(__name__)


class MfmaSpider(scrapy.Spider):
    name = "mfma"
    allowed_domains = ["mfma.treasury.gov.za"]
    start_urls = ["http://mfma.treasury.gov.za"]

    def __init__(self, start_url=None, scrape_menu="true"):
        self.base = "http://mfma.treasury.gov.za"

        self.form_table_css = (
            "div.mainContent > table > tr > td#MSOZoneCell_WebPartWPQ2"
        )
        self.simple_content_css = ".mainContent"

        if start_url:
            self.start_urls = [start_url]

        self.should_scrape_menu = scrape_menu == "true"

    def parse(self, response):
        if self.should_scrape_menu:
            self.should_scrape_menu = False
            for item in self.scrape_menu(response):
                yield item

        for item in self.page_item(response):
            yield item

    def scrape_menu(self, response):
        menu_item = MenuItem()
        menu_item["type"] = "menu"
        menu_items = []
        for menu_link in response.selector.css("#zz1_QuickLaunchMenu a"):
            url = menu_link.xpath("@href").extract()[0]
            if "AllDocuments" in url:
                continue
            menu_items.append(
                {
                    "url": self.dedotnet(url, indexhtml=False),
                    "text": menu_link.xpath("text()").extract()[0].strip(),
                }
            )

            if urlparse.urlparse(url).scheme:
                abs_url = url
            else:
                abs_url = urlparse.urljoin(response.url, url)
            yield scrapy.Request(abs_url)
        menu_item["menu_items"] = menu_items
        yield menu_item

    def page_item(self, response):
        page_item = PageItem()
        page_item["type"] = "page"
        page_item["form_table_rows"] = []

        if response.selector.css(self.form_table_css):
            for item in self.set_form_table_content(page_item, response):
                yield item
        elif response.selector.css(self.simple_content_css):
            for item in self.set_simple_content(page_item, response):
                yield item

        title_css = ".breadcrumbCurrent"
        if response.selector.css(title_css):
            page_item["title"] = (
                response.selector.css(title_css).xpath("text()")[0].extract()
            )
        yield page_item

    def set_form_table_content(self, page_item, response):
        rows = get_rows(response)

        url = response.url
        if self.is_forms_url(url):
            url = self.fix_forms_url(url)
        purl = urlparse.urlparse(url)
        location = self.dedotnet(purl.path, indexhtml=False)
        page_item["original_url"] = url
        page_item["path"] = location

        for row in rows:
            label = row.xpath(".//tr/td/a/text()")[0].extract()
            row_href = row.xpath(".//tr/td/a/@href")[0].extract()
            path = decode_url_root_folder(row_href)
            user_xpath = './/td[@class="ms-vb-user"]/text()'
            user = row.xpath(user_xpath)[0].extract()
            mod_date_xpath = './/td[@class="ms-vb2"]/nobr/text()'
            mod_date = row.xpath(mod_date_xpath)[0].extract()
            row_item = {
                "type": "table_form_item",
                "label": label,
                "path": path,
                "modified_date": mod_date,
                "user": user,
            }
            page_item["form_table_rows"].append(row_item)

            if self.has_file_extension(path):
                file_item = FileItem()
                file_item["original_url"] = urlparse.urljoin(response.url, path)
                file_item["path"] = urllib.unquote(path)
                file_item["type"] = "file"
                yield file_item
            else:
                child = "http://%s%s" % (purl.netloc, path)
                yield scrapy.Request(child)

        nextlink = response.xpath('//img[@alt="Next"]')
        if nextlink:
            qs = urllib.urlencode({"p_FileLeafRef": label, "Paged": "TRUE"})
            next_page_url = urlparse.urljoin(url, "?" + qs)
            yield scrapy.Request(next_page_url)

        breadcrumbs_css = "#ctl00_PlaceHolderTitleBreadcrumb_ContentMap"
        css_match = response.selector.css(breadcrumbs_css)
        if css_match:
            page_item["breadcrumbs"] = self.breadcrumbs_html(css_match)
        raise StopIteration

    @staticmethod
    def has_file_extension(path):
        regex = "^.+(\..{1,4})$"
        return re.match(regex, path)

    def set_simple_content(self, page_item, response):
        url = urlparse.urlparse(response.url)
        page_item["original_url"] = response.url
        page_item["path"] = self.dedotnet(url.path)
        body = response.selector.css(self.simple_content_css)[0].extract()
        for x in self.fix_body(page_item, body):
            yield x

        breadcrumbs_css = "#ctl00_PlaceHolderTitleBreadcrumb_siteMapPath"
        css_match = response.selector.css(breadcrumbs_css)
        if css_match:
            page_item["breadcrumbs"] = self.breadcrumbs_html(css_match)

    def breadcrumbs_html(self, match):
        breadcrumbs_html = match[0].extract()
        breadcrumbs_html = self.fix_links(breadcrumbs_html)
        soup = BeautifulSoup(breadcrumbs_html, "html.parser")
        for a in soup.findAll("a"):
            a["href"] = self.dedotnet(a["href"], indexhtml=False)
        return str(soup)

    def fix_body(self, page_item, html):
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.findAll("a"):
            try:
                url = a["href"]
                if self.is_forms_url(url):
                    url = self.fix_forms_url(url)
                purl = urlparse.urlparse(url)
                if purl.scheme == "mailto":
                    continue
                if purl.hostname:
                    abs_url = url
                else:
                    abs_url = self.base + url

                if (
                    self.has_file_extension(purl.path)
                    and not purl.path.endswith("aspx")
                    and not purl.hostname
                ):
                    a["href"] = abs_url
                    file_item = FileItem()
                    file_item["original_url"] = abs_url
                    file_item["path"] = urllib.unquote(purl.path)
                    file_item["type"] = "file"
                    yield file_item
                elif "Authenticate" in url:
                    continue
                elif purl.hostname == "mfma.treasury.gov.za" or not purl.hostname:
                    a["href"] = self.dedotnet(purl.path)
                    yield scrapy.Request(abs_url)
                else:
                    pass
            except KeyError:
                import pdb

                pdb.set_trace

        body = self.clean_html(str(soup))
        page_item["body"] = body

    def fix_links(self, html):
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.findAll("a"):
            try:
                url = a["href"]
                purl = urlparse.urlparse(url)
                if not purl.hostname:
                    url = self.base + url
                if self.is_forms_url(url):
                    url = self.fix_forms_url(url)
                a["href"] = url
            except KeyError:
                import pdb

                pdb.set_trace

        return str(soup)

    def clean_html(self, html):
        soup = BeautifulSoup(html, "html.parser")
        whitelist = {"src", "href", "target", "alt"}
        for a in soup.findAll("a"):
            for tag in soup.findAll(True):
                for a in tag.attrs.keys():
                    if a not in whitelist:
                        del tag.attrs[a]
        html = str(soup)
        html = re.sub(r"</?br>\s*</?br>(\s*</?br>)*", "<br><br>", html)
        return html

    def is_forms_url(self, url):
        parsed = urlparse.urlparse(url)
        return "RootFolder" in url

    def fix_forms_url(self, url):
        parsed_url = urlparse.urlparse(url)
        parsed_qs = urlparse.parse_qs(parsed_url.query)
        if "RootFolder" in parsed_qs:
            parsed_rootfolder = urlparse.urlparse(parsed_qs["RootFolder"][0])
            if parsed_url.netloc:
                return "http://%s%s" % (parsed_url.netloc, parsed_rootfolder.path)
            else:
                return parsed_rootfolder.path
        else:
            return url

    def dedotnet(self, path, indexhtml=True, trailing_slash=True):
        if indexhtml:
            replacement = "/index.html"
        else:
            replacement = "/" if trailing_slash else ""
        path = path.replace("/Pages/Default.aspx", replacement)
        path = path.replace("/Pages/default.aspx", replacement)
        path = path.replace("/Forms/AllItems.aspx", replacement)
        path = path.replace(".aspx", replacement)
        return path


def get_rows(response):
    return response.css(".ms-vb-title .ms-unselectedtitle").xpath("../..")


def decode_url_root_folder(url):
    querystring = urlparse.urlsplit(url).query
    return urlparse.parse_qs(querystring)["RootFolder"][0]

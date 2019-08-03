import os
from unittest import TestCase
from scrapy.http import HtmlResponse, Request
from mfma.spiders import mfma_spider
from mfma.items import MenuItem, PageItem


class ScrapeMenuTestCase(TestCase):
    def setUp(self):
        with open(
            os.path.join(
                os.path.splitext(__file__)[0],
                self.__class__.__name__ + "_page_source.html",
            )
        ) as page_source_file:
            page_source = page_source_file.read()

        self.response = HtmlResponse("http://mfma.treasury.gov.za/", body=page_source)
        self.spider = mfma_spider.MfmaSpider()

    def test_scrape_menu(self):
        items = list(self.spider.scrape_menu(self.response))
        requests = [i for i in items if type(i) == Request]
        menu_items = [i for i in items if type(i) == MenuItem]
        self.assertEqual(1, len(menu_items))
        # Check that at least one link was found and added correctly
        link_item = [
            l for l in menu_items[0]["menu_items"] if l["text"] == "MFMA Learning"
        ][0]

        self.assertEqual(link_item["url"], "/Pages/MFMALearning/")
        self.assertEqual(15, len(requests))
        # Check some request was yielded correctly
        self.assertEqual(
            1,
            len(
                [
                    r
                    for r in requests
                    if r.url == "http://mfma.treasury.gov.za/Pages/MFMALearning.aspx"
                ]
            ),
        )


class FormTableContentTestCase(TestCase):
    def setUp(self):
        with open(
            os.path.join(
                os.path.splitext(__file__)[0],
                self.__class__.__name__ + "_page_source.html",
            )
        ) as page_source_file:
            page_source = page_source_file.read()

        self.response = HtmlResponse("http://mfma.treasury.gov.za/", body=page_source)
        self.spider = mfma_spider.MfmaSpider()
        self.page_item = PageItem()
        self.page_item["type"] = "page"
        self.page_item["form_table_rows"] = []

    def test_set_form_table_content(self):
        items = list(self.spider.set_form_table_content(self.page_item, self.response))
        self.assertEqual(7, len(self.page_item["form_table_rows"]))
        self.assertEqual(7, len(items))

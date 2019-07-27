import os
from unittest import TestCase
from scrapy.http import HtmlResponse
from mfma.spiders import mfma_spider


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
        self.assertEqual(5, len(items))

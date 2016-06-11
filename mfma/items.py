# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy

class MenuItem(scrapy.Item):
    menu_items = scrapy.Field()

class PageItem(scrapy.Item):
    title = scrapy.Field()
    body = scrapy.Field()
    menu = scrapy.Field()
    breadcrumbs = scrapy.Field()

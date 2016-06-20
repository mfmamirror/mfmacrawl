# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class MenuItem(scrapy.Item):
    type = scrapy.Field()
    menu_items = scrapy.Field()


class PageItem(scrapy.Item):
    type = scrapy.Field()
    original_url = scrapy.Field()
    path = scrapy.Field()
    title = scrapy.Field()
    body = scrapy.Field()
    menu = scrapy.Field()
    breadcrumbs = scrapy.Field()


class TableFormItem(scrapy.Item):
    type = scrapy.Field()
    path = scrapy.Field()
    label = scrapy.Field()
    user = scrapy.Field()
    modified_date = scrapy.Field()
    location = scrapy.Field()

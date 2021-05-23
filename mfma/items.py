"""
Scrapy spider that receives page items and probably just one menu item
and builds/updates a jekyll website from those items to mirror the scraped site.

This is quite specific to the site being mirrored but a lot of cool stuff can be
be learned from this to mirror other sites.
"""

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
    form_table_rows = scrapy.Field()
    breadcrumbs = scrapy.Field()


class FileItem(scrapy.Item):
    type = scrapy.Field()
    original_url = scrapy.Field()
    path = scrapy.Field()

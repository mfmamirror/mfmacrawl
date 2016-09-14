# -*- coding: utf-8 -*-
"""
Scrapy spider that receives page items and probably just one menu item
and builds/updates a jekyll website from those items to mirror the scraped site.

This is quite specific to the site being mirrored but a lot of cool stuff can be
be learned from this to mirror other sites.
"""

import json
import os
import codecs
import yaml
import re


class MirrorBuilderPipeline(object):

    def __init__(self, mirror_path):
        self.mirror = MFMAMirror(mirror_path)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(mirror_path=crawler.settings.get('MFMA_MIRROR_PATH', None))

    def open_spider(self, spider):
        pass

    def close_spider(self, spider):
        pass

    def process_item(self, item, spider):
        if item['type'] == 'page':
            self.mirror.write_page(item)
        elif item['type'] == 'menu':
            self.mirror.make_menu(item)

        return item


class MFMAMirror(object):

    def __init__(self, path):
        self.path = path

    def write_menu(self, menu):
        jsonstr = json.dumps(menu['menu_items'])
        self.write_file('_data/menu.json', jsonstr)

    def write_page(self, page):
        table_items = page['form_table_rows']

        for item in table_items:
            if self.has_file_extension(item['path']):
                item['path'] = 'http://mfma.treasury.gov.za' + item['path']

        preamble_data = {
            'title': page.get('title' ''),
            'breadcrumbs': page.get('breadcrumbs', ''),
            'layout': 'default',
            'original_url': page['original_url'],
            'table_items': page['form_table_rows'],
        }

        preamble_yaml = yaml.safe_dump(preamble_data)
        content = page.get('body', '')
        pagestr = "---\n%s\n---\n%s" % (preamble_yaml, content)
        filename = page['path'] + '/index.html'
        self.write_file(filename, pagestr)

    def write_file(self, filename, data):
        filename = self.path + filename
        directory = os.path.dirname(filename)
        if not os.path.exists(directory):
            os.makedirs(directory)
        with codecs.open(filename, 'w', encoding='utf8') as file:
            file.write(data)

    @staticmethod
    def has_file_extension(path):
        regex = '^.+(\..{1,4})$'
        return re.match(regex, path)

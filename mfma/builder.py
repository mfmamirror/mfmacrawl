"""
MFMA Mirror builder
"""

import codecs
import json
import os
import pdb
import re
import sys
import traceback
import urllib
import yaml


class Builder():
    def __init__(self, path):
        self.path = path

    def handle_item(self, item):
        if item['type'] == 'page':
            self.write_page(item)
        elif item['type'] == 'menu':
            self.write_menu(item)
        else:
            pass

    def write_menu(self, menu):
        jsonstr = json.dumps(menu['menu_items'])
        self.write_file('_data/menu.json', jsonstr)

    def write_page(self, page):
        table_items = page['form_table_rows']

        for item in table_items:
            if self.has_file_extension(item['path']):
                item['path'] = 'http://mfmamirror.s3.amazonaws.com' + item['path']
            if '%20' in item['path']:
                item['path'] = urllib.unquote(item['path'])

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
        if '%20' in page['path']:
            page['path'] = urllib.unquote(page['path'])
        if '/index.html' in page['path']:
            filename = page['path']
        else:
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


def main():
    [jsonpath, output_dir] = sys.argv[1:]
    builder_ = Builder(output_dir)
    try:
        with open(jsonpath, 'r') as jsonlines:
            for itemjson in jsonlines.readlines():
                item = json.loads(itemjson)
                builder_.handle_item(item)
    except:
        type, value, tb = sys.exc_info()
        traceback.print_exc()
        pdb.post_mortem(tb)


if __name__ == "__main__":
    main()

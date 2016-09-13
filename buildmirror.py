import sys
import traceback
import pdb
import os
import json
import yaml
import codecs
from itertools import groupby


def build(jsonfile, output_dir):
    items = json.load(jsonfile)
    menu = [item for item in items if item['type'] == 'menu']
    if menu:
        make_menu(output_dir, menu[0])

    table_form_items = [item for item in items if item['type'] == 'table_form_item']
    path_items = make_table_form_datasets(table_form_items)

    pages = [item for item in items if item['type'] == 'page']
    make_pages(output_dir, pages, path_items)


def make_menu(output_dir, menu):
    jsonstr = json.dumps(menu['menu_items'])
    write_file(output_dir + '/_data/menu.json', jsonstr)


def make_pages(output_dir, pages, path_items):
    for page in pages:
        preamble_data = {
            'title': page.get('title' ''),
            'breadcrumbs': page.get('breadcrumbs', ''),
            'layout': 'default',
            'original_url': page['original_url'],
        }
        dir = page['path'].replace('/index.html', '')
        if dir in path_items:
            preamble_data['table_items'] = path_items[dir]
        preamble_yaml = yaml.safe_dump(preamble_data)
        content = page.get('body', '')
        pagestr = "---\n%s\n---\n%s" % (preamble_yaml, content)
        write_file(output_dir + page['path'], pagestr)


def make_table_form_datasets(table_form_items):
    path_items = {}
    pathf = lambda i: i['location']
    sorted_items = sorted(table_form_items, key=pathf)
    for path, items in groupby(sorted_items, pathf):
        item_list = list(items)
        path_items[path] = item_list
        for item in item_list:
            path = item['path']
            if path.lower().endswith('.pdf') \
               or path.lower().endswith('.xls') \
               or path.lower().endswith('.xlsx') \
               or path.lower().endswith('.doc') \
               or path.lower().endswith('.docx') \
               or path.lower().endswith('.ppt') \
               or path.lower().endswith('.pptx') \
               or path.lower().endswith('.db') \
               or path.lower().endswith('.txt') \
               or path.lower().endswith('.log') \
               or path.lower().endswith('.xlsm') \
               or path.lower().endswith('.tmp') \
               or path.lower().endswith('.msg'):
                item['path'] = 'http://mfma.treasury.gov.za' + path
    return path_items


def write_file(filename, data):
    directory = os.path.dirname(filename)
    if not os.path.exists(directory):
        os.makedirs(directory)
    with codecs.open(filename, 'w', encoding='utf8') as file:
        file.write(data)


def main():
    [jsonpath, output_dir] = sys.argv[1:]
    try:
        with open(jsonpath, 'r') as jsonfile:
            build(jsonfile, output_dir)
    except:
        type, value, tb = sys.exc_info()
        traceback.print_exc()
        pdb.post_mortem(tb)


if __name__ == "__main__":
    main()

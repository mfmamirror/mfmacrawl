import sys
import traceback
import pdb
import os
import json
import yaml
import codecs


def build(jsonfile, output_dir):
    items = json.load(jsonfile)
    menu = [item for item in items if item['type'] == 'menu'][0]
    make_menu(output_dir, menu)
    pages = [item for item in items if item['type'] == 'page']
    for page in pages:
        preamble_data = {
            'title': page.get('title' ''),
            'breadcrumbs': page.get('breadcrumbs', ''),
            'layout': 'default',
            'original_url': page['original_url'],
        }
        preamble_yaml = yaml.safe_dump(preamble_data)
        content = page.get('body', '')
        pagestr = "---\n%s\n---\n%s" % (preamble_yaml, content)
        write_file(output_dir + page['path'], pagestr)


def make_menu(output_dir, menu):
    jsonstr = json.dumps(menu['menu_items'])
    write_file(output_dir + '/_data/menu.json', jsonstr)


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

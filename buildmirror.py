import sys
import traceback
import pdb
import os
import json
import yaml


def build(jsonfile, output_dir):
    items = json.load(jsonfile)
    menu = [item for item in items if item['type'] == 'menu'][0]
    make_menu(output_dir, menu)
    pages = [item for item in items if item['type'] == 'page']


def make_menu(output_dir, menu):
    jsonstr = json.dumps(menu['menu_items'])
    write_file(output_dir + '/_data/menu.json', jsonstr)


def write_file(filename, data):
    directory = os.path.dirname(filename)
    if not os.path.exists(directory):
        os.makedirs(directory)
    with open(filename, 'w') as file:
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

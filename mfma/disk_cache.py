from scrapy.utils.project import project_data_dir
import hashlib
import logging
import os
from os.path import exists


logger = logging.getLogger(__name__)


class DiskCache(object):
    def __init__(self, name):
        self.name = name
        self.cachedir = os.path.join(project_data_dir(), name)
        if not exists(self.cachedir):
            os.makedirs(self.cachedir)

    def get(self, key):
        path = self.prepare_path(key)
        if not exists(path):
            logger.info("Cache %s miss %s", self.name, key)
            return None
        else:
            with open(path) as f:
                value = f.read()
                logger.info("Cache %s hit %s %r", self.name, key, value)
                return value

    def put(self, key, value):
        logger.info("Cache %s update %s %r", self.name, key, value)
        path = self.prepare_path(key)
        with open(path, 'w') as f:
            f.write(value)

    def prepare_path(self, key):
        filename = hashlib.sha256(key.encode('utf8')).hexdigest()
        pathdir = os.path.join(self.cachedir, filename[:2])
        if not exists(pathdir):
            os.makedirs(pathdir)
        path = os.path.join(pathdir, filename)
        return path

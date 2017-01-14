# -*- coding: utf-8 -*-
"""
Scrapy spider that receives page items and probably just one menu item
and builds/updates a jekyll website from those items to mirror the scraped site.

This is quite specific to the site being mirrored but a lot of cool stuff can be
be learned from this to mirror other sites.
"""

from boto.s3.key import Key
from mfma.items import FileItem
from tempfile import NamedTemporaryFile
import boto
import logging
import requests

logger = logging.getLogger(__name__)


class DepagingPipeline(object):
    """
    Last page item wins, so overwrite page with this page plus previous page's
    table items
    """

    def __init__(self):
        self.page_rows = {}

    def process_item(self, item, spider):
        if item['type'] == 'page':
            rows = self.page_rows.get(item['path'], [])
            rows.extend(item['form_table_rows'])
            item['form_table_rows'] = rows
            self.page_rows[item['path']] = rows
        return item


class FileArchivePipeline(object):
    def __init__(self, s3_bucket_name, aws_key_id, aws_key_secret):
        self.s3_bucket_name = s3_bucket_name
        self.aws_key_id = aws_key_id
        self.aws_key_secret = aws_key_secret
        self.conn = None
        self.bucket = None

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            s3_bucket_name=crawler.settings.get('S3_BUCKET_NAME'),
            aws_key_id=crawler.settings.get('AWS_KEY_ID'),
            aws_key_secret=crawler.settings.get('AWS_KEY_SECRET'),
        )

    def open_spider(self, spider):
        self.conn = boto.connect_s3(self.aws_key_id, self.aws_key_secret)
        self.bucket = self.conn.get_bucket(self.s3_bucket_name)

    def process_item(self, item, spider):
        if isinstance(item, FileItem):
            logger.info("Archiving %s to %s", item['original_url'], item['path'])
            key_str = item['path']
            key = self.bucket.get_key(key_str)
            if key:
                logger.info("%s already exists in s3", key_str)
            else:
                with NamedTemporaryFile(delete=True) as fd:
                    logger.info("Downloading %s", item['original_url'])
                    r = requests.get(item['original_url'], stream=True)
                    r.raise_for_status()
                    for chunk in r.iter_content(chunk_size=None):
                        fd.write(chunk)
                    logger.info("Uploading %s", item['path'])
                    key = Key(
                        self.bucket,
                        name=key_str,
                    )
                    key.content_type = r.headers['content-type']
                    key.set_contents_from_file(fd, rewind=True)
                    key.make_public()
        return item

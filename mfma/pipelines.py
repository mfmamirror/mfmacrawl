# -*- coding: utf-8 -*-
"""
Scrapy spider that receives page items and probably just one menu item
and builds/updates a jekyll website from those items to mirror the scraped site.

This is quite specific to the site being mirrored but a lot of cool stuff can be
be learned from this to mirror other sites.
"""

from boto.s3.connection import OrdinaryCallingFormat
from boto.s3.key import Key
from datetime import datetime
from mfma.items import FileItem
from os.path import basename, splitext
from scrapinghub import Connection, Project
from tempfile import NamedTemporaryFile
import boto
import logging
import re
import requests

logger = logging.getLogger(__name__)

MFMA_RIGHTS = 'These National Treasury publications may not be reproduced wholly or in part without the express authorisation of the National Treasury in writing unless used for non-profit purposes.'
MFMA_DOC_KEYWORDS = 'Local Government;MFMA;Municipal Financial Management Act;Finance;Governance;Management;National;Local;Government;Planning;South Africa;Provincial'

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


class S3FileArchivePipeline(object):
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
                etag = key.get_metadata('upstream-etag') or ''
            else:
                key = Key(
                    self.bucket,
                    name=key_str,
                )
                etag = ''
            with NamedTemporaryFile(delete=True) as fd:
                logger.info("Requesting %s", item['original_url'])
                headers = {'if-none-match': etag}
                r = requests.get(item['original_url'], stream=True, headers=headers)
                if r.status_code == 304:
                    logger.info("%s already exists in s3 and is up to date", key_str)
                elif r.status_code == 200:
                    for chunk in r.iter_content(chunk_size=None):
                        fd.write(chunk)
                    logger.info("Uploading %s", item['path'])
                    key.set_metadata('upstream-etag', r.headers['etag'])
                    key.set_metadata('last-modified', r.headers['last-modified'])
                    key.set_metadata('content-type', r.headers['content-type'])
                    key.set_contents_from_file(fd, rewind=True)
                    key.make_public()
                else:
                    r.raise_for_status()
        return item


class InternetArchiveFileArchivePipeline(object):
    def __init__(self, key_id, key_secret):
        self.key_id = key_id
        self.key_secret = key_secret
        self.conn = None
        self.day_of_week = int(datetime.now().strftime("%w"))

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            key_id=crawler.settings.get('INTERNET_ARCHIVE_KEY_ID'),
            key_secret=crawler.settings.get('INTERNET_ARCHIVE_KEY_SECRET'),
        )

    def open_spider(self, spider):
        logger.info("connecting to IAS3")
        self.conn = boto.connect_s3(
            self.key_id,
            self.key_secret,
            host='s3.us.archive.org',
            is_secure=False,
            calling_format=OrdinaryCallingFormat()
        )

    def process_item(self, item, spider):
        if isinstance(item, FileItem):
            identifier = item['path']
            identifier.replace(' ', '_')
            # - identifier must be alphanum, - or _
            # - identifier must be 5-100 chars long
            # - identifier must be lower case
            identifier = re.sub('[^\w-]+', '_', identifier).lower()[-100:]
            # identifier must start with alphanum
            identifier = re.sub('^[^a-z0-9]+', '', identifier)
            logger.info("Archiving %s to %s", item['original_url'], identifier)
            key_str = item['path']
            try:
                bucket = self.conn.get_bucket(identifier)
                key = bucket.get_key(key_str)
            except boto.exception.S3ResponseError, e:
                if e.code == 'NoSuchBucket':
                    key = None
                else:
                    raise e
            if key:
                etag = key.get_metadata('upstream-etag') or ''
            else:
                title = splitext(basename(item['path']))[0]
                r = requests.head(item['original_url'])
                r.raise_for_status()
                headers = {
                    'x-archive-meta-content-type': r.headers['content-type'],
                    'x-archive-meta-date': datetime.strptime(r.headers['last-modified'], '%a, %d %b %Y %H:%M:%S %Z'),
                    'x-archive-meta-description': item['path'],
                    'x-archive-meta-licenseurl': 'http://mfma.treasury.gov.za',
                    'x-archive-meta-mediatype': 'text',
                    'x-archive-meta-publisher': 'National Treasury, Republic of South Africa',
                    'x-archive-meta-rights': MFMA_RIGHTS,
                    'x-archive-meta-subject': MFMA_DOC_KEYWORDS,
                    'x-archive-meta-title': title,
                    'x-archive-meta-collection': 'mfmasouthafrica',
                }
                bucket = self.conn.create_bucket(identifier, headers=headers)
                key = Key(bucket, name=key_str,)
                etag = ''
            with NamedTemporaryFile(delete=True) as fd:
                logger.info("Requesting %s", item['original_url'])
                headers = {'if-none-match': etag}
                r = requests.get(item['original_url'], stream=True, headers=headers)
                if r.status_code == 304:
                    logger.info("%s already exists at archive.org and is up to date", key_str)
                elif r.status_code == 200:
                    for chunk in r.iter_content(chunk_size=None):
                        fd.write(chunk)
                    logger.info("Uploading %s to archive.org identifier %s", item['path'], identifier)
                    key.set_metadata('last-modified', r.headers['last-modified'])
                    key.set_metadata('upstream-etag', r.headers['etag'])
                    key.set_contents_from_file(fd, rewind=True)
                else:
                    r.raise_for_status()
        return item

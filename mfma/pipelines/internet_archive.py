from boto.s3.connection import OrdinaryCallingFormat
from boto.s3.key import Key
import boto
from mfma.disk_cache import DiskCache
from scrapy.pipelines.media import MediaPipeline
from io import BytesIO
from mfma.items import FileItem
import logging
from scrapy.http import Request
from scrapy.utils.request import referer_str
from botocore.exceptions import ClientError
import re
from os.path import basename, splitext, exists
import requests
from datetime import datetime
from tempfile import NamedTemporaryFile


MFMA_RIGHTS = 'These National Treasury publications may not be reproduced wholly or in part without the express authorisation of the National Treasury in writing unless used for non-profit purposes.'
MFMA_DOC_KEYWORDS = 'Local Government;MFMA;Municipal Financial Management Act;Finance;Governance;Management;National;Local;Government;Planning;South Africa;Provincial'


logger = logging.getLogger(__name__)


class InternetArchiveFileArchivePipeline(object):
    def __init__(self, key_id, key_secret):
        self.download_func = None  # A MediaPipeline expected attribute
        self.handle_httpstatus_list = None  # A MediaPipeline expected attribute
        self.key_id = key_id
        self.key_secret = key_secret
        self.etag_cache = DiskCache('internet-archive-file-archive')

    @classmethod
    def from_crawler(cls, crawler):
        cls.crawler = crawler # a MediaPipeline expectation
        return cls(
            key_id=crawler.settings.get('INTERNET_ARCHIVE_KEY_ID'),
            key_secret=crawler.settings.get('INTERNET_ARCHIVE_KEY_SECRET'),
        )

    def open_spider(self, spider):
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
            etag = self.etag_cache.get(key_str)
            if etag:
                key = None
            else:
                try:
                    bucket = self.conn.get_bucket(identifier)
                    key = bucket.get_key(key_str)
                    logger.info(f"Key { key } for { key_str }")
                except boto.exception.S3ResponseError as e:
                    if e.code == 'NoSuchBucket':
                        logger.info(f"Key { key_str } does not exist yet.")
                        key = None
                    else:
                        raise e
                if key:
                    etag = key.get_metadata('upstream-etag') or ''
                    if etag:
                        self.etag_cache.put(key_str, etag)
                else:
                    # The file doesn't exist in IA. Create it (bucket per file)
                    title = splitext(basename(item['path']))[0]

                    r = requests.head(item['original_url'], allow_redirects=True)
                    r.raise_for_status()
                    headers = {
                        'x-archive-keep-old-version': '1',
                        'x-archive-meta-content-type': r.headers['content-type'],
                        'x-archive-meta-date': str(datetime.strptime(r.headers['last-modified'], '%a, %d %b %Y %H:%M:%S %Z')),
                        'x-archive-meta-description': item['path'],
                        'x-archive-meta-licenseurl': 'http://mfma.treasury.gov.za',
                        'x-archive-meta-mediatype': 'text',
                        'x-archive-meta-publisher': 'National Treasury, Republic of South Africa',
                        'x-archive-meta-rights': MFMA_RIGHTS,
                        'x-archive-meta-subject': MFMA_DOC_KEYWORDS,
                        'x-archive-meta-title': title,
                        'x-archive-meta-collection': 'mfmasouthafrica',
                    }
                    try:
                        bucket = self.conn.create_bucket(identifier, headers=headers)
                    except boto.exception.S3CreateError as e:
                        if e.code == 'BucketAlreadyExists':
                            logger.info(f"Thought bucket didn't exist but got key conflict so it's probably ok ({ key_str })")
                        else:
                            raise e
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
                    if not key:
                        bucket = self.conn.get_bucket(identifier)
                        key = bucket.get_key(key_str)
                    key.set_metadata('last-modified', r.headers['last-modified'])
                    if 'etag' in r.headers:
                        key.set_metadata('upstream-etag', r.headers['etag'])
                    key.set_contents_from_file(fd, rewind=True)
                    if 'etag' in r.headers:
                        self.etag_cache.put(key_str, r.headers['etag'])
                else:
                    r.raise_for_status()
        return item

# IAS3 has a bucket per item and one or more file per item.
# The bucket name is the item identifier.
#
# Due to the normalisation and possible shortening needed
# to make the path work as an identifier, it's possible for
# the occasional identifier conflict. Since the etag is stored
# as file metadata and the file key is the full path of the file,
# we should at least not overwrite or repeatedly unnecessarily upload files.
#
# The file path upstream is the file key in the bucket.
#
# We treat each unique file on mfma.treasury.gov.za as a unique item.
# The file path is translated into an item identifier.
#
# We store the upstream etag as file metadata (not bucket metadata).
#
# 1. calculate the bucket name and key string based on file path
# 2. see if we have an etag cached for it
#    a. if we have it in the on-disk cache, assume internet archive's etag is the same
#    b. else try and fetch the etag from internet archive
#        3. fetch the bucket
#        4. fetch the key
# 4. request the file from upstream (including its etag if we have one)
# 5. if the latest version isn't archived, upload the latest version
#    a. create the bucket, tolerating conflicts
#    b. create the key
#    c. upload the file, with flag to keep multiple versions


from boto.s3.connection import OrdinaryCallingFormat
from boto.s3.key import Key
from botocore.exceptions import ClientError
from datetime import datetime
from io import BytesIO
from mfma.disk_cache import DiskCache
from mfma.items import FileItem
from os.path import basename, splitext, exists
from scrapy.http import Request
from scrapy.pipelines.media import MediaPipeline
from scrapy.pipelines.media import MediaPipeline
from scrapy.utils.request import referer_str
from tempfile import NamedTemporaryFile
import boto
import logging
import re
import requests


MFMA_RIGHTS = 'These National Treasury publications may not be reproduced wholly or in part without the express authorisation of the National Treasury in writing unless used for non-profit purposes.'
MFMA_DOC_KEYWORDS = 'Local Government;MFMA;Municipal Financial Management Act;Finance;Governance;Management;National;Local;Government;Planning;South Africa;Provincial'


logger = logging.getLogger(__name__)


class InternetArchiveFileArchivePipeline(MediaPipeline):
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
        super(InternetArchiveFileArchivePipeline, self).open_spider(spider)
        self.conn = boto.connect_s3(
            self.key_id,
            self.key_secret,
            host='s3.us.archive.org',
            is_secure=False,
            # OrdinaryCallingFormat places the bucket name in the path
            # as opposed to the default SubdomainCallingFormat which treats
            # the bucket as a subdomain of the hostname
            calling_format=OrdinaryCallingFormat()
        )

    def get_media_requests(self, item, info):
        if isinstance(item, FileItem):
            identifier = path_identifier(item['path'])
            logger.info("Archiving %s to %s", item['original_url'], identifier)
            key_str = item['path']
            etag = self.etag_cache.get(key_str)
            bucket = None
            if not etag:
                try:
                    bucket = self.conn.get_bucket(identifier)
                    key = bucket.get_key(key_str)
                    if key:
                        etag = key.get_metadata('upstream-etag') or ''
                    if etag:
                        self.etag_cache.put(key_str, etag)
                    logger.info(f"Key { key } for { key_str }")
                except boto.exception.S3ResponseError as e:
                    if e.code == 'NoSuchBucket':
                        logger.info(f"Bucket { identifier } does not exist yet.")
                    else:
                        raise e

            headers = {'If-None-Match': etag}
            logger.info(f"Requesting {item['original_url']}")
            meta = {
                "key_str": key_str,
                "bucket": bucket,
                # the scrapy cache seems to be interfering with our etag/if-none-match
                # submission and we don't need to cache it when using if-none-match
                # with the etag in the archive anyway
                "dont_cache": True,
            }
            return [Request(item['original_url'], headers=headers, meta=meta)]
        else:
            return []

    def media_downloaded(self, response, request, info, *, item=None):
        key_str = response.meta["key_str"]
        bucket = response.meta["bucket"]
        identifier = path_identifier(key_str)

        if response.status == 304:
            logger.info("%s already exists at archive.org and is up to date", key_str)
        elif response.status == 200:
            try:
                last_modified = response.headers['last-modified'].decode("utf-8")
                content_type = response.headers['content-type'].decode("utf-8")
                if 'etag' in response.headers:
                    etag = response.headers['etag'].decode("utf-8")
                else:
                    etag = None

                if not bucket:
                    title = splitext(basename(item['path']))[0]
                    bucket = self.create_bucket(
                        content_type,
                        last_modified,
                        item['path'],
                        title,
                        identifier
                    )
                key = Key(bucket, name=key_str)

                with NamedTemporaryFile(delete=False) as fd:
                    fd.write(response.body)
                    fd.flush()
                    key.set_metadata('last-modified', last_modified)
                    if etag:
                        key.set_metadata('upstream-etag', etag)

                    logger.info(f"Uploading {item['path']} to archive.org identifier {identifier}")
                    try:
                        key.set_contents_from_file(fd, rewind=True)
                    except boto.exception.S3ResponseError as e:
                        if e.code == "BadContent" and len(response.body) == 0:
                            logger.error("Invalid file from upstream rejected by internet archive")
                        else:
                            raise e

                    # after successful upload, update cached etag
                    if etag:
                        self.etag_cache.put(key_str, etag)
            except Exception as e:
                logger.exception(f"e", exc_info=True)

        else:
            referer = referer_str(request)
            logger.warning(
                'File (code: %(status)s): Error downloading file from '
                '%(request)s referred in <%(referer)s>',
                {'status': response.status,
                 'request': request, 'referer': referer},
                extra={'spider': info.spider}
            )
            raise Exception(f'Error downloading {key_str}')

    def create_bucket(self, content_type, last_modified, description, title, identifier):
        """Return a bucket or raise an exception."""

        bucket_headers = {
            'x-archive-keep-old-version': '1',
            'x-archive-meta-content-type': content_type,
            'x-archive-meta-date': str(datetime.strptime(last_modified, '%a, %d %b %Y %H:%M:%S %Z')),
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
            return self.conn.create_bucket(identifier, headers=bucket_headers)
        except boto.exception.S3CreateError as e:
            if e.code == 'BucketAlreadyExists':
                logger.info((f"Thought bucket didn't exist but got key"
                             f"conflict so it's probably ok ({ key_str })"))
                return self.conn.get_bucket(identifier)
            else:
                raise e


def path_identifier(path):
    identifier = path
    identifier.replace(" ", "_")
    # - identifier must be alphanum, - or _
    # - identifier must be 5-100 chars long
    # - identifier must be lower case
    identifier = re.sub("[^\w-]+", "_", identifier).lower()[-100:]
    # identifier must start with alphanum
    identifier = re.sub("^[^a-z0-9]+", "", identifier)
    return identifier

# 1. calculate they file key based on its upstream path
# 2. see if we have an etag for it
#    a. if we have it in the on-disk cache, assume
# 3. request it including its etag
# 4. if the latest version isn't archived, upload the latest version

from boto.s3.key import Key
import boto
from mfma.disk_cache import DiskCache
from scrapy.pipelines.media import MediaPipeline
from io import BytesIO
from mfma.items import FileItem
import logging
from scrapy.http import Request
from scrapy.utils.request import referer_str

logger = logging.getLogger(__name__)


class S3FileArchivePipeline(MediaPipeline):
    def __init__(self, s3_bucket_name, aws_key_id, aws_key_secret):
        self.download_func = None  # A MediaPipeline expected attribute
        self.handle_httpstatus_list = None
        self.s3_bucket_name = s3_bucket_name
        self.aws_key_id = aws_key_id
        self.aws_key_secret = aws_key_secret
        self.conn = None
        self.bucket = None
        self.etag_cache = DiskCache('s3-file-archive')

    @classmethod
    def from_crawler(cls, crawler):
        cls.crawler = crawler # a MediaPipeline expectation
        return cls(
            s3_bucket_name=crawler.settings.get('S3_BUCKET_NAME'),
            aws_key_id=crawler.settings.get('AWS_KEY_ID'),
            aws_key_secret=crawler.settings.get('AWS_KEY_SECRET'),
        )

    def open_spider(self, spider):
        super(S3FileArchivePipeline, self).open_spider(spider)
        self.conn = boto.connect_s3(self.aws_key_id, self.aws_key_secret)
        self.bucket = self.conn.get_bucket(self.s3_bucket_name)


    def get_media_requests(self, item, info):
        if isinstance(item, FileItem):
            logger.info("Archiving %s to %s", item['original_url'], item['path'])
            key_str = item['path']
            etag = self.etag_cache.get(key_str)
            if etag:
                key = None
            else:
                key = self.bucket.get_key(key_str)
                if key:
                    etag = key.get_metadata('upstream-etag') or ''
                    if etag:
                        self.etag_cache.put(key_str, etag)
                else:
                    key = Key(
                        self.bucket,
                        name=key_str,
                    )
                    etag = ''
            logger.info("Requesting %s", item['original_url'])
            headers = {'if-none-match': etag}
            meta = {
                "key": key,
                "key_str": key_str,
            }
            return [Request(item['original_url'], headers=headers, meta=meta)]
        else:
            return []

    def media_downloaded(self, response, request, info, *, item=None):
        key_str = response.meta["key_str"]
        key = response.meta["key"]

        if response.status == 304:
            logger.info("%s already exists in s3 and is up to date", key_str)
        elif response.status == 200:
            logger.info("Uploading %s", item['path'])
            if not key:
                key = self.bucket.get_key(key_str)
            if 'etag' in response.headers:
                key.set_metadata('upstream-etag', response.headers['etag'])
            key.set_metadata('last-modified', response.headers['last-modified'])
            key.set_metadata('content-type', response.headers['content-type'])
            buf = BytesIO(response.body)
            key.set_contents_from_file(buf, rewind=True)
            key.make_public()
            if 'etag' in r.headers:
                self.etag_cache.put(key_str, r.headers['etag'])
        else:
            referer = referer_str(request)
            logger.warning(
                'File (code: %(status)s): Error downloading file from '
                '%(request)s referred in <%(referer)s>',
                {'status': response.status,
                 'request': request, 'referer': referer},
                extra={'spider': info.spider}
            )
            raise FileException('download-error')

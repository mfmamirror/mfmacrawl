# 1. calculate they file key based on its upstream path
# 2. see if we have an etag for it
#    a. if we have it in the on-disk cache, assume s3's etag is the same
#    b. else try and fetch the etag from s3
# 3. request it including its etag if we have one
# 4. if the latest version isn't archived, upload the latest version

import boto3
from mfma.disk_cache import DiskCache
from scrapy.pipelines.media import MediaPipeline
from io import BytesIO
from mfma.items import FileItem
import logging
from scrapy.http import Request
from scrapy.utils.request import referer_str
from botocore.exceptions import ClientError


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
        self.s3 = boto3.client(
            "s3",
            region_name="eu-west-1",
            aws_access_key_id=self.aws_key_id,
            aws_secret_access_key=self.aws_key_secret
        )

    def get_media_requests(self, item, info):
        if isinstance(item, FileItem):
            logger.info("Archiving %s to %s", item['original_url'], item['path'])
            key_str = item['path'][1:] # strip root /
            etag = self.etag_cache.get(key_str)
            if not etag:
                try:
                    heads = self.s3.head_object(Bucket=self.s3_bucket_name, Key=key_str)
                    etag = heads.get('x-amz-meta-upstream-etag', '')
                    if etag:
                        self.etag_cache.put(key_str, etag)
                except ClientError as ex:
                    if ex.response['Error']['Code'] == '404':
                        logger.info('got Not Found when doing HEAD')
                    else:
                        logger.info(f"Unexpected error fetching metadata {ex.response}")
                        raise ex
            headers = {'If-None-Match': etag}
            logger.info(f"Requesting {item['original_url']}")
            meta = {
                "key_str": key_str,
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

        if response.status == 304:
            logger.info("%s already exists in s3 and is up to date", key_str)
        elif response.status == 200:
            logger.info(f"Uploading {item['path']}")
            last_modified = response.headers['last-modified'].decode("utf-8")
            content_type = response.headers['content-type'].decode("utf-8")
            etag = response.headers['etag'].decode("utf-8")
            meta = {
                'last-modified': last_modified,
            }
            if 'etag' in response.headers:
                meta['upstream-etag'] = etag
            try:
                self.s3.put_object(
                    ACL="public-read",
                    Key=key_str,
                    Bucket=self.s3_bucket_name,
                    Metadata=meta,
                    ContentType=content_type,
                    Body=BytesIO(response.body)
                )
            except Exception as e:
                logger.exception(f"e", exc_info=True)
            # Update cache with latest etag
            if 'etag' in response.headers:
                self.etag_cache.put(key_str, etag)
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

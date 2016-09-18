# -*- coding: utf-8 -*-
"""
Scrapy spider that receives page items and probably just one menu item
and builds/updates a jekyll website from those items to mirror the scraped site.

This is quite specific to the site being mirrored but a lot of cool stuff can be
be learned from this to mirror other sites.
"""

from builder import Builder
from git import Repo
from git import Actor
from scrapy.utils.project import data_path
import logging
import os

logger = logging.getLogger(__name__)
URI = "https://%s@github.com/mfmamirror/mfmamirror.github.io.git"


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


class MirrorBuilderPipeline(object):

    def __init__(self, mirror_path, basic_auth_creds):
        if mirror_path:
            self.mirror_path = mirror_path
        else:
            self.mirror_path = data_path('mfmamirror')
        self.basic_auth_creds = basic_auth_creds

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mirror_path=crawler.settings.get('MFMA_MIRROR_PATH', None),
            basic_auth_creds=crawler.settings.get('MFMA_GIT_BASIC_AUTH', None),
        )

    def open_spider(self, spider):
        logger.info("Starting up %s " % self.__class__.__name__)
        self.mirror = MFMAMirror(URI % self.basic_auth_creds, self.mirror_path)

    def close_spider(self, spider):
        self.mirror.upload()

    def process_item(self, item, spider):
        self.mirror.builder_.handle_item(item)
        return item


class MFMAMirror(object):

    def __init__(self, uri, path):
        self.builder_ = Builder(path)

        if os.path.exists(self.path):
            self.repo = Repo(self.path)
            logger.info("Found existing mirror repo at %s" % self.path)
        else:
            os.makedirs(self.path)
            logger.info("Cloning mirror repo into %s" % self.path)
            self.repo = Repo.clone_from(uri, self.path, depth=1)
            logger.info("Done cloning mirror repo into %s" % self.path)

    def upload(self):
        actor = Actor("MFMAMirror scraper", "jbothma+mfmaspider@gmail.com")
        diffs = self.repo.index.diff(None)
        logger.info("%d changed files" % len(diffs))
        self.repo.index.add([diff.a_blob.path for diff in diffs])
        logger.info("%d new files" % len(self.repo.untracked_files))
        self.repo.index.add([path for path in self.repo.untracked_files])
        self.repo.index.commit(
            "scrapy spider committed this",
            author=actor,
            committer=actor
        )
        logger.info("pushing changes to mirror")
        self.repo.remotes.origin.push()
        logger.info("done pushing changes to mirror")

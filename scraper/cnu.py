#!/usr/bin/env python
# @Author: eilianxiao
# @Date: Dec 26 18:44 2020
import json
from pathlib import Path
from typing import List

import aiofiles
import typer
from ruia import AttrField, Item, Spider, TextField

from scraper.utils import mkdirs_if_not_exist, safe_filename

IMAGE_HOST = 'http://imgoss.cnu.cc/'
AUTHOR_RCMDS_PREFIX = 'http://www.cnu.cc/users/recommended/'
AUTHOR_WORKS_PREFIX = 'http://www.cnu.cc/users/'
WORK_PREFIX = 'http://www.cnu.cc/works/'
THUMBNAIL_SUFFIX = '?x-oss-process=style/content'
PAGE_SUFFIX = '?page={page}'

APP_NAME = 'CNU Scraper'
BASE_DIR = 'www.cnu.cc'
START_URLS = [
    'http://www.cnu.cc/works/{id}',  # 作品集 URL
    'http://www.cnu.cc/users/{id}',  # 用户作品页 URL
    'http://www.cnu.cc/users/recommended/{id}',  # 用户推荐页 URL
]
DESTINATION = Path('.')
OVERWRITE = False
THUMBNAIL = False
WORKER_NUMBERS = 2
CONCURRENCY = 25
RETRIES = 3
DELAY = 0
RETRY_DELAY = 0
TIMEOUT = 20


class PageItem(Item):
    target_item = TextField(css_select='div.pager_box')
    max_page = TextField(css_select='ul>li:nth-last-child(2)', default=1)


class WorkItem(Item):
    target_item = TextField(css_select='div.work-thumbnail')
    author = TextField(css_select='div.author')
    title = TextField(css_select='div.title')  # WorkPage 中是日期
    work = AttrField(css_select='.thumbnail', attr='href')


class ImagesItem(Item):
    target_item = TextField(css_select='body')
    author = TextField(css_select='.author-info strong')
    title = TextField(css_select='.work-title')
    imgs_json = TextField(css_select='#imgs_json')


class CNUSpider(Spider):
    name = APP_NAME
    start_urls = START_URLS
    request_config = {
        'RETRIES': RETRIES,
        'DELAY': 0,
        'TIMEOUT': TIMEOUT
    }
    concurrency = CONCURRENCY
    # aiohttp config
    aiohttp_kwargs = {}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._destination = DESTINATION
        self._overwrite = OVERWRITE
        self._thumbnail = THUMBNAIL
        # 更新 Spider 及自定义的配置
        for k, v in kwargs.get('spider_config', {}).items():
            setattr(self, k, v)

    async def parse(self, response):
        if response.url.startswith(AUTHOR_WORKS_PREFIX):
            async for page_item in PageItem.get_items(html=await response.text()):
                for page in range(1, int(page_item.max_page) + 1):
                    page_url = f'{response.url.split("?")[0]}{PAGE_SUFFIX.format(page=page)}'
                    yield self.request(
                        url=page_url,
                        metadata={
                            'current_page': page,
                            'max_page': page_item.max_page,
                        },
                        callback=self.parse_page)
        elif response.url.startswith(WORK_PREFIX):
            yield self.parse_work(response)
        else:
            self.logger.warning(f'Parser not support URL: {response.url}')

    async def parse_page(self, response):
        async for work_item in WorkItem.get_items(html=await response.text()):
            yield self.request(
                url=work_item.work,
                metadata={
                    'current_page': response.metadata['current_page'],
                    'max_page': response.metadata['max_page'],
                    'author': work_item.author,
                    'title': work_item.title,
                    'work': work_item.work
                },
                callback=self.parse_work
            )

    async def parse_work(self, response):
        async for images_item in ImagesItem.get_items(html=await response.text()):
            urls = [IMAGE_HOST + img.get('img') for img in json.loads(images_item.imgs_json)]
            for index, url in enumerate(urls):
                basename = url.split('/')[-1]
                save_dir = (self._destination /
                            BASE_DIR /
                            safe_filename(images_item.author) /
                            safe_filename(images_item.title))
                fpath = save_dir / f'[{index + 1:02d}]{basename}'
                if self._overwrite or not fpath.is_file():
                    if self._thumbnail:
                        url += THUMBNAIL_SUFFIX
                    self.logger.info(f'Downloading {url} ...')
                    yield self.request(
                        url=url,
                        metadata={
                            'title': images_item.title,
                            'index': index,
                            'url': url,
                            'basename': basename,
                            'save_dir': save_dir,
                            'fpath': fpath
                        },
                        callback=self.save_image
                    )
                else:
                    self.logger.info(f'Skipped already exists: {fpath}')

    async def save_image(self, response):
        # 创建图片保存目录
        save_dir = response.metadata['save_dir']
        if mkdirs_if_not_exist(save_dir):
            self.logger.info(f'Created directory: {save_dir}')
        # 保存图片
        fpath = response.metadata['fpath']
        try:
            content = await response.read()
        except TypeError as e:
            self.logger.error(e)
        else:
            async with aiofiles.open(fpath, 'wb') as f:
                await f.write(content)
                self.logger.info(f'Saved to {fpath}')


def cnu_command(
        start_urls: List[str] = typer.Argument(
            ...,
            help='URLs of the works'
        ),
        destination: Path = typer.Option(
            DESTINATION, '-d', '--destination',
            help='Destination directory to save the images'
        ),
        overwrite: bool = typer.Option(
            OVERWRITE, '-o / -no', '--overwrite / --no-overwrite',
            help='Whether to overwrite existing images'
        ),
        thumbnail: bool = typer.Option(
            THUMBNAIL, '-t', '--thumbnail',
            help='Whether to download the thumbnail images'
        ),
        retries: int = typer.Option(
            RETRIES, '-r', '--retries',
            help='Number of retries when the download fails'
        ),
        worker_numbers: int = typer.Option(
            WORKER_NUMBERS, '-w', '--workers',
            help='Number of parallel workers'
        ),
        concurrency: int = typer.Option(
            CONCURRENCY, '-c', '--concurrency',
            help='Number of concurrency'
        ),
        delay: int = typer.Option(
            DELAY, '--delay',
            help='Seconds to wait for the next request'
        ),
        retry_delay: int = typer.Option(
            RETRY_DELAY, '--retry-delay',
            help='Seconds to wait for the retry request'
        ),
        timeout: int = typer.Option(
            TIMEOUT, '--timeout',
            help='Seconds of HTTP request timeout'
        ),
):
    """ A scraper to download images from http://www.cnu.cc/"""
    # 开始爬虫任务
    CNUSpider.start(
        spider_config=dict(
            start_urls=list(start_urls),
            request_config={
                'RETRIES': retries,
                'DELAY': delay,
                'RETRY_DELAY': retry_delay,
                'TIMEOUT': timeout
            },
            _destination=destination,
            _overwrite=overwrite,
            _thumbnail=thumbnail,
            worker_numbers=worker_numbers,
            concurrency=concurrency
        )
    )

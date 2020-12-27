#!/usr/bin/env python
# @Author: eilianxiao
# @Date: Dec 26 18:44 2020
import json
from pathlib import Path
from typing import List

import aiofiles
import typer
from ruia import Item, Spider, TextField

from scraper.utils import mkdirs_if_not_exist, safe_filename

IMAGE_HOST = 'http://imgoss.cnu.cc/'
BASE_DIR = 'www.cnu.cc'
THUMBNAIL_PARAMS = '?x-oss-process=style/content'

APP_NAME = 'CNU Scraper'
START_URLS = ['http://www.cnu.cc/works/{work_id}']
DESTINATION = Path('.')
CONCURRENCY = 10
OVERWRITE = False
RETRIES = 3
TIMEOUT = 20.0
THUMBNAIL = False


class CNUItem(Item):
    target_item = TextField(css_select='body')
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
        async for item in CNUItem.get_items(html=response.html):
            urls = [IMAGE_HOST + img.get('img') for img in json.loads(item.imgs_json)]
            for index, url in enumerate(urls):
                basename = url.split('/')[-1]
                save_dir = self._destination / BASE_DIR / safe_filename(item.title)
                fpath = save_dir / f'[{index + 1:02d}]{basename}'
                if self._overwrite or not fpath.is_file():
                    if self._thumbnail:
                        url += THUMBNAIL_PARAMS
                    self.logger.info(f'Downloading {url} ...')
                    yield self.request(
                        url=url,
                        metadata={
                            'title': item.title,
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

    async def process_item(self, item):
        pass

    async def save_image(self, response):
        # 创建图片保存目录
        save_dir = response.metadata['save_dir']
        if mkdirs_if_not_exist(save_dir):
            self.logger.info(f'Created directory: {save_dir}')
        # 保存图片
        fpath = response.metadata['fpath']
        async with aiofiles.open(fpath, 'wb') as f:
            await f.write(response.html)
            self.logger.info(f'Saved to {fpath}')


def multi_spider_start():
    import asyncio

    async def start():
        await asyncio.gather(
            CNUSpider.async_start(cancel_tasks=False),
            CNUSpider.async_start(cancel_tasks=False),
        )
        await CNUSpider.cancel_all_tasks()

    asyncio.get_event_loop().run_until_complete(start())


def cnu_command(
        start_urls: List[str] = typer.Argument(..., help='URLs of the works'),
        destination: Path = typer.Option(DESTINATION, help='Destination directory to save the images'),
        overwrite: bool = typer.Option(OVERWRITE, help='Whether to overwrite existing images'),
        retries: int = typer.Option(RETRIES, help='Times of retries when the download fails'),
        concurrency: int = typer.Option(CONCURRENCY, help='Maximum number of parallel workers'),
        timeout: float = typer.Option(TIMEOUT, help='HTTP request timeout, in seconds'),
        thumbnail: bool = typer.Option(THUMBNAIL, help='Whether to download the thumbnail image')
):
    """ A scraper to download images from http://www.cnu.cc/"""
    # 开始爬虫任务
    CNUSpider.start(spider_config=dict(
        start_urls=list(start_urls),
        request_config={
            'RETRIES': retries,
            'DELAY': 0,
            'TIMEOUT': timeout
        },
        _destination=destination,
        _overwrite=overwrite,
        _thumbnail=thumbnail,
        concurrency=concurrency,
    ))

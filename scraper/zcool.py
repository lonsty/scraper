# -*- coding: utf-8 -*-
# @Author: lonsty
# @Date:   2019-09-07 18:34:18
# @Last Modified by:   lonsty
# @Last Modified time: 2019-09-08 02:58:36
import json
import os
import re
import sys
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor, wait, as_completed
from datetime import datetime
from queue import Empty, Queue
from threading import Thread
import time
from urllib.parse import urljoin, urlparse
from uuid import uuid4

import click
import requests
from bs4 import BeautifulSoup

from scraper.utils import convert_to_safe_filename, mkdirs_if_not_exist, retry

Scrapy = namedtuple('Scrapy', 'type author title url')
HOST_PAGE = 'https://www.zcool.com.cn'
PAGE_SUFFIX = '?myCate=0&sort=1&p={page}'
USER_SUFFIX = '/u/{id}'
SEARCH_DESIGNER_SUFFIX = '/search/designer?&word={word}'
TIMEOUT = (10, 20)
Q_TIMEOUT = 5
MAX_WORKERS = 20
RETRIES = 3


class ZCoolScraper():

    def __init__(self, user_id=None, username=None, directory=None, max_pages=None, max_topics=None,
                 max_workers=None, retries=None, redownload=None, override=None, proxies=None):
        self.start_time = datetime.now()
        print(f'\n *** {self.start_time.ctime()} ***\n')

        self.max_topics = max_topics or 'all'
        self.max_workers = max_workers or MAX_WORKERS
        self.pool = ThreadPoolExecutor(self.max_workers)
        self.override = override
        self.pages = Queue()
        self.topics = Queue()
        self.images = Queue()
        self.stat = {
            'npages': 0,
            'ntopics': 0,
            'nimages': 0,
            'pages_pass': set([]),
            'pages_fail': set([]),
            'topics_pass': set([]),
            'topics_fail': set([]),
            'images_pass': set([]),
            'images_fail': set([])
        }

        if retries:
            global RETRIES
            RETRIES = retries

        if isinstance(proxies, str):
            try:
                self.proxies = json.loads(proxies)
            except Exception:
                print(f'Proxies <{proxies}> Invalid!')
                sys.exit(1)
        else:
            self.proxies = None

        if redownload:
            self.username = self._reload_records(redownload)
            self.user_id = self._search_id_by_username(self.username)
            self.max_pages = self.pages.qsize()
            self.max_topics = self.topics.qsize()
            self.directory = os.path.abspath(os.path.join(directory or '', urlparse(HOST_PAGE).netloc,
                                                          convert_to_safe_filename(self.username)))
            self.stat.update({
                'npages': self.max_pages,
                'ntopics': self.max_topics,
                'nimages': self.images.qsize(),
            })
            print(f'Username: {self.username}\n'
                  f'ID: {self.user_id}\n'
                  f'Pages to scrapy: {self.max_pages:2d}\n'
                  f'Topics to scrapy: {self.max_topics:3d}\n'
                  f'Images to scrapy: {self.images.qsize():4d}\n'
                  f'Storage directory: {self.directory}', end='\n\n')
            return

        self.user_id = user_id or self._search_id_by_username(username)
        self.base_url = urljoin(HOST_PAGE, USER_SUFFIX.format(id=self.user_id))

        try:
            response = requests.get(self.base_url, proxies=self.proxies, timeout=TIMEOUT)
        except Exception:
            print(f'Failed to connect to {HOST_PAGE}')
            sys.exit(1)
        soup = BeautifulSoup(markup=response.text, features='html.parser')

        try:
            author = soup.find(name='div', id='body').get('data-name')
            if username and username != author:
                print('Wrong <user id> or <username>!')
                sys.exit(1)
            self.username = author
        except Exception:
            self.username = username or 'anonymous'
        self.directory = os.path.abspath(os.path.join(directory or '', urlparse(HOST_PAGE).netloc,
                                                      convert_to_safe_filename(self.username)))

        try:
            max_page = int(soup.find(id='laypage_0').find_all(name='a')[-2].text)
        except Exception:
            max_page = 1
        self.max_pages = min(max_pages or 9999, max_page)

        print(f'Username: {self.username}\n'
              f'ID: {self.user_id}\n'
              f'Maximum pages: {max_page}\n'
              f'Pages to scrapy: {self.max_pages}\n'
              f'Topics to scrapy: {"all" if self.max_topics == "all" else (self.max_pages * self.max_topics)}\n'
              f'Storage directory: {self.directory}', end='\n\n')
        self._fetch_all()

    def _reload_records(self, file):
        with open(file, 'r', encoding='utf-8') as ff:
            for fail in json.loads(ff.read()).get('fail'):
                scrapy = Scrapy._make(fail.values())
                if scrapy.type == 'page':
                    self.pages.put(scrapy)
                elif scrapy.type == 'topic':
                    self.topics.put(scrapy)
                elif scrapy.type == 'image':
                    self.images.put(scrapy)
            return scrapy.author

    def _search_id_by_username(self, username):
        if not username:
            print('Must give a <user id> or <username>!')
            sys.exit(1)
        try:
            response = requests.get(urljoin(HOST_PAGE, SEARCH_DESIGNER_SUFFIX.format(word=username)),
                                    proxies=self.proxies, timeout=TIMEOUT)
        except Exception:
            print(f'Failed to connect to {HOST_PAGE}')
            sys.exit(1)

        author_1st = BeautifulSoup(response.text, 'html.parser').find(name='div', class_='author-info')
        if (not author_1st) or (author_1st.get('data-name') != username):
            print(f'User <{username}> not exist!')
            sys.exit(1)

        id = author_1st.get('data-id')
        return id

    def _fetch_all(self):
        fetch_future = [self.pool.submit(self._generate_all_pages),
                        self.pool.submit(self._fetch_all_topics),
                        self.pool.submit(self._fetch_all_images)]
        end_show_fetch = False
        t = Thread(target=self._show_fetch_status, kwargs={'end': lambda: end_show_fetch})
        t.start()
        wait(fetch_future)
        end_show_fetch = True
        t.join()

    def _generate_all_pages(self):
        for i in range(1, self.max_pages + 1):
            url = urljoin(self.base_url, PAGE_SUFFIX.format(page=i))
            scrapy = Scrapy(type='page', author=self.username, title=i, url=url)
            if scrapy not in self.stat["pages_pass"]:
                self.pages.put(scrapy)

    def _fetch_all_topics(self):
        page_future = {}
        while True:
            try:
                scrapy = self.pages.get(timeout=Q_TIMEOUT)
                if scrapy not in self.stat["pages_pass"]:
                    page_future[self.pool.submit(self.parse_topics, scrapy)] = scrapy
            except Empty:
                break
            except Exception:
                continue
        for idx, future in enumerate(as_completed(page_future)):
            scrapy = page_future.get(future)
            try:
                future.result()
                self.stat["pages_pass"].add(scrapy)
            except Exception as exc:
                self.stat["pages_fail"].add(scrapy)

    def _fetch_all_images(self):
        image_future = {}
        while True:
            try:
                scrapy = self.topics.get(timeout=Q_TIMEOUT)
                if scrapy not in self.stat["topics_pass"]:
                    image_future[self.pool.submit(self.parse_images, scrapy)] = scrapy
            except Empty:
                break
            except Exception:
                continue

        for idx, future in enumerate(as_completed(image_future)):
            scrapy = image_future.get(future)
            try:
                future.result()
                self.stat["topics_pass"].add(scrapy)
            except Exception:
                self.stat["topics_fail"].add(scrapy)

    def _show_fetch_status(self, interval=0.5, end=None):
        while True:
            print(f'Fetched Pages: {self.max_pages:2d}\t'
                  f'Topics: {self.stat["ntopics"]:3d}\t'
                  f'Images: {self.stat["nimages"]:4d}', end='\r', flush=True)
            if (interval == 0) or (end and end()):
                print('\n')
                break
            time.sleep(interval)

    def _show_download_status(self, interval=0.5, end=None):
        while True:
            print(f'Time used: {str(datetime.now() - self.start_time)[:-7]}\t'
                  f'Failed: {len(self.stat["images_fail"]):3d}\t'
                  f'Completed: {len(self.stat["images_pass"]) + len(self.stat["images_fail"])}'
                  f'/{self.stat["nimages"]}', end='\r', flush=True)
            if (interval == 0) or (end and end()):
                print('\n')
                break
            time.sleep(interval)

    def run_scraper(self):
        end_show_download = False
        t = Thread(target=self._show_download_status, kwargs={'end': lambda: end_show_download})
        t.start()

        image_futures = {}
        while True:
            try:
                scrapy = self.images.get_nowait()
                if scrapy not in self.stat["images_pass"]:
                    image_futures[self.pool.submit(self.download_image, scrapy)] = scrapy
                else:
                    pass
            except Empty:
                break
            except Exception:
                continue

        for idx, future in enumerate(as_completed(image_futures)):
            scrapy = image_futures.get(future)
            try:
                if future.result():
                    self.stat["images_pass"].add(scrapy)
                else:
                    self.stat["images_fail"].add(scrapy)
            except Exception:
                self.stat["images_fail"].add(scrapy)

        end_show_download = True
        t.join()

        saved_images = len(self.stat["images_pass"])
        failed_images = len(self.stat["images_fail"])
        if saved_images or failed_images:
            if saved_images:
                print(f'Saved {saved_images:3d} images to {self.directory}')
            records_path = self.save_records()
            print(f'Saved records to {records_path}')
        else:
            print('No images to download.')

    @retry(Exception, tries=RETRIES)
    def parse_topics(self, scrapy):
        resp = requests.get(scrapy.url, proxies=self.proxies, timeout=TIMEOUT)
        if resp.status_code != 200:
            raise Exception(f'Response status code: {resp.status_code}')

        cards = BeautifulSoup(resp.text, 'html.parser').find_all(name='a', class_='card-img-hover')
        for card in (cards if self.max_topics == 'all' else cards[:self.max_topics + 1]):
            new_scrapy = Scrapy('topic', scrapy.author, card.get('title'), card.get('href'))
            if new_scrapy not in self.stat["topics_pass"]:
                self.topics.put(new_scrapy)
                self.stat["ntopics"] += 1
        return scrapy

    @retry(Exception, tries=RETRIES)
    def parse_images(self, scrapy):
        resp = requests.get(scrapy.url, proxies=self.proxies, timeout=TIMEOUT)
        if resp.status_code != 200:
            raise Exception(f'Response status code: {resp.status_code}')

        soup = BeautifulSoup(markup=resp.text, features='html.parser')
        for div in soup.find_all(name='div', class_='reveal-work-wrap text-center'):
            url = div.find(name='img').get('src')
            new_scrapy = Scrapy('image', scrapy.author, scrapy.title, url)
            if new_scrapy not in self.stat["images_pass"]:
                self.images.put(new_scrapy)
                self.stat["nimages"] += 1
        return scrapy

    @retry(Exception, tries=RETRIES)
    def download_image(self, scrapy):
        try:
            name = re.findall(r'(?<=/)\w*?\.jpg|\.png', scrapy.url, re.IGNORECASE)[0]
        except IndexError:
            name = uuid4().hex + '.jpg'

        path = os.path.join(self.directory, convert_to_safe_filename(scrapy.title))
        filename = os.path.join(path, name)
        if (not self.override) and os.path.isfile(filename):
            return scrapy

        resp = requests.get(scrapy.url, proxies=self.proxies, timeout=TIMEOUT)
        if resp.status_code != 200:
            raise Exception(f'Response status code: {resp.status_code}')

        mkdirs_if_not_exist(path)
        with open(filename, 'wb') as fi:
            fi.write(resp.content)
        return scrapy

    @retry(Exception, tries=RETRIES)
    def save_records(self):
        filename = f'{convert_to_safe_filename(self.start_time.isoformat()[:-7])}.json'
        abspath = os.path.abspath(os.path.join(self.directory, filename))
        with open(abspath, 'w', encoding='utf-8') as ff:
            records = {
                'time': self.start_time.isoformat(),
                'success': [scrapy._asdict() for scrapy in
                            (self.stat["pages_pass"] | self.stat["topics_pass"] | self.stat["images_pass"])],
                'fail': [scrapy._asdict() for scrapy in
                         (self.stat["pages_fail"] | self.stat["topics_fail"] | self.stat["images_fail"])]
            }
            ff.write(json.dumps(records, ensure_ascii=False, indent=4))
        return abspath


@click.command()
@click.option('-i', '--id', 'id', help='User id.')
@click.option('-u', '--username', 'name', help='User name.')
@click.option('-d', '--directory', 'dir', help='Directory to save images.')
@click.option('-p', '--max-pages', 'max_pages', type=int, help='Maximum pages to parse.')
@click.option('-t', '--max-topics', 'max_topics', type=int, help='Maximum topics per page to parse.')
@click.option('-w', '--max-workers', 'max_workers', default=MAX_WORKERS, show_default=True,
              type=int, help='Maximum thread workers.')
@click.option('-R', '--retries', 'retries', default=RETRIES, show_default=True,
              type=int, help='Repeat download for failed images.')
@click.option('-r', '--redownload', 'redownload', help='Redownload images from failed records.')
@click.option('-o', '--override', 'override',  is_flag=True, show_default=True, help='Override existing files.')
@click.option('--proxies', help='Use proxies to access websites.\nExample:\n{"http": "user:passwd'
                                '@www.example.com:port",\n"https": "user:passwd@www.example.com:port"}')
def zcool_command(id, name, dir, max_pages, max_topics, max_workers,
                  retries, redownload, override, proxies):
    """Use multi-threaded to download images from https://www.zcool.com.cn in bulk by username or ID."""
    if not any([id, name, redownload]):
        click.echo('Must give a <id> or <username>!')
        sys.exit(1)

    scraper = ZCoolScraper(id, name, dir, max_pages, max_topics, max_workers,
                           retries, redownload, override, proxies)
    try:
        scraper.run_scraper()
    except KeyboardInterrupt:
        click.echo('\n\nKeyboard Interruption.')
        sys.exit(1)

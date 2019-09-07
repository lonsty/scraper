# -*- coding: utf-8 -*-
# @Author: Allen
# @Date:   2019-09-07 18:34:18
# @Last Modified by:   Allen
# @Last Modified time: 2019-09-07 22:56:48
import os
import re
import sys
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime
from functools import wraps
from queue import Empty, Queue
from time import sleep
from urllib.parse import urljoin, urlparse
from uuid import uuid4

import click
import requests

from bs4 import BeautifulSoup

Scrapy = namedtuple('Scrapy', 'type author title url')
HOST_PAGE = 'https://www.zcool.com.cn'
PAGE_SUFFIX = '?myCate=0&sort=1&p={page}'
USER_SUFFIX = '/u/{id}'
SEARCH_DESIGNER_SUFFIX = '/search/designer?&word={word}'


def print_status(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        result = func(self, *args, **kwargs)
        print(f'Time spent: {str(datetime.now() - self.start_time)[:-7]} \tRemaining tasks: '
              f'{self.to_crawl.qsize():5d} \tCompleted tasks: {len(self.scraped):5d}', end='\r')
        return result

    return wrapper


class MultiThreadScraper():

    def __init__(self, user_id=None, username=None, directory=None, max_pages=None, max_topics=None, max_workers=None):
        self.user_id = user_id or self.search_id_by_username(username)
        self.max_topics = max_topics or 'all'
        self.max_workers = max_workers or 20
        self.base_url = HOST_PAGE + USER_SUFFIX.format(id=self.user_id)
        self.pool = ThreadPoolExecutor(self.max_workers)
        self.scraped = set([])
        self.to_crawl = Queue()

        response = requests.get(self.base_url, timeout=(5, 10))
        soup = BeautifulSoup(markup=response.text, features='html.parser')
        try:
            author = soup.find(name='div', id='body').get('data-name')
            if username and username != author:
                raise ValueError('Wrong <user id> or <username>!')
            self.username = author
        except Exception:
            self.username = username or 'anonymous'
        self.directory = os.path.abspath(os.path.join(directory or '', urlparse(HOST_PAGE).netloc,
                                                      self.convert_to_safe_filename(self.username)))

        try:
            max_page = int(soup.find(id='laypage_0').find_all(name='a')[-2].text)
        except Exception:
            max_page = 1
        self.max_pages = min(max_pages or 9999, max_page)

        for i in range(1, self.max_pages + 1):
            url = urljoin(self.base_url, PAGE_SUFFIX.format(page=i))
            scrapy = Scrapy(type='page', author=self.username, title=i, url=url)
            if scrapy not in self.scraped:
                self.to_crawl.put(scrapy)

        print(f'Username: {self.username}')
        print(f'ID: {self.user_id}')
        print(f'Max pages: {max_page}')
        print(f'Pages to scrapy: {self.max_pages}')
        print(f'Topics to scrapy: {"all" if self.max_pages == "all" else (self.max_pages * self.max_topics)}')
        print(f'Storage directory: {self.directory}')
        self.start_time = datetime.now()
        print(self.start_time.ctime(), end='\n\n')

    @staticmethod
    def mkdirs_if_not_exist(dir):
        if not os.path.isdir(dir):
            try:
                os.makedirs(dir)
            except FileExistsError:
                pass

    @staticmethod
    def convert_to_safe_filename(filename):
        return "".join([c for c in filename if c not in r'\/:*?"<>|']).strip()

    @staticmethod
    def search_id_by_username(username):
        if not username:
            raise ValueError('Must give a <user id> or <username>!')
        try:
            response = requests.get(urljoin(HOST_PAGE, SEARCH_DESIGNER_SUFFIX.format(word=username)), timeout=(5, 10))
        except Exception:
            print(f'Failed to connect to {HOST_PAGE}')
            sys.exit(1)

        author_1st = BeautifulSoup(response.text, 'html.parser').find(name='div', class_='author-info')
        if (not author_1st) or (author_1st.get('data-name') != username):
            print(f'User <{username}> not exist!')
            sys.exit(1)

        id = author_1st.get('data-id')
        return id

    @print_status
    def scrape_page(self, scrapy):
        try:
            res = requests.get(scrapy.url, timeout=(3, 10))
            return scrapy, res
        except requests.RequestException:
            return

    @print_status
    def parse_topics(self, scrapy, html):
        soup = BeautifulSoup(markup=html.text, features='html.parser')
        for card in soup.find_all(name='a', class_='card-img-hover')[:self.max_topics + 1]:
            new_scrapy = Scrapy('topic', scrapy.author, card.get('title'), card.get('href'))
            if new_scrapy not in self.scraped:
                self.to_crawl.put(new_scrapy)
        self.scraped.add(scrapy)

    @print_status
    def parse_images(self, scrapy, html):
        soup = BeautifulSoup(markup=html.text, features='html.parser')
        for div in soup.find_all(name='div', class_='reveal-work-wrap text-center'):
            url = div.find(name='img').get('src')
            new_scrapy = Scrapy('image', scrapy.author, scrapy.title, url)
            if new_scrapy not in self.scraped:
                self.to_crawl.put(new_scrapy)
        self.scraped.add(scrapy)

    @print_status
    def save_image(self, scrapy, html):
        path = os.path.join(self.directory, self.convert_to_safe_filename(scrapy.title))
        self.mkdirs_if_not_exist(path)
        try:
            name = re.findall(r'(?<=/)\w*?\.jpg|\.png', scrapy.url, re.IGNORECASE)[0]
        except IndexError:
            name = uuid4().hex + '.jpg'
        with open(os.path.join(path, name), 'wb') as fi:
            fi.write(html.content)
        self.scraped.add(scrapy)

    def post_scrape_callback(self, res):
        result = res.result()
        if not result or result[1].status_code != 200:
            return
        scrapy, response = result
        if scrapy.type == 'page':
            self.parse_topics(scrapy, response)
        elif scrapy.type == 'topic':
            self.parse_images(scrapy, response)
        elif scrapy.type == 'image':
            self.save_image(scrapy, response)

    def run_scraper(self):
        futures = []
        while True:
            try:
                target_scrapy = self.to_crawl.get(timeout=3)
                if target_scrapy not in self.scraped:
                    # self.scraped.add(target_scrapy)
                    job = self.pool.submit(self.scrape_page, target_scrapy)
                    job.add_done_callback(self.post_scrape_callback)
                    futures.append(job)
            except Empty:
                break
            except Exception as e:
                print(e)
                continue
        wait(futures)
        sleep(5)
        saved_images = len([1 for s in self.scraped if s.type == "image"])
        if saved_images:
            print(f'\n\nSaved {saved_images} images to {self.directory}.')


@click.command()
@click.option('-i', '--id', 'id', help='User id.')
@click.option('-u', '--username', 'name', help='User name.')
@click.option('-d', '--directory', 'dir', help='Directory to save images.')
@click.option('-p', '--max-pages', 'max_pages', type=int, help='Max pages to parse.')
@click.option('-t', '--max-topics', 'max_topics', type=int, help='Max topics per page to parse.')
@click.option('-w', '--max-workers', 'max_workers', default=12, show_default=True,
              type=int, help='Max thread workers.')
def command(id, name, dir, max_pages, max_topics, max_workers):
    """Use multi-threaded to download images from https://www.zcool.com.cn in bulk by username or ID."""
    if not any([id, name]):
        click.echo('Must give a <id> or <username>!')
        sys.exit(1)

    scraper = MultiThreadScraper(id, name, dir, max_pages, max_topics, max_workers)
    try:
        scraper.run_scraper()
    except KeyboardInterrupt:
        click.echo('\n\nKeyboard Interruption.')
        sys.exit(1)


if __name__ == '__main__':
    command()

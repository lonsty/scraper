# -*- coding: utf-8 -*-
# @Author: Allen
# @Date:   2019-09-07 18:34:18
# @Last Modified by:   Allen
# @Last Modified time: 2019-09-07 18:35:08
import os
import re
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor, wait
from queue import Queue, Empty
from urllib.parse import urljoin, urlparse
from uuid import uuid4

import requests
from bs4 import BeautifulSoup

Scrapy = namedtuple('Scrapy', 'type author title url')
SUFFIX = '?myCate=0&sort=1&p={page}'


class MultiThreadScraper():

    def __init__(self, home_url, save_path=''):
        scheme, netloc, *_ = urlparse(home_url)
        self.base_url = f'{scheme}://{netloc}'
        self.save_path = os.path.join(save_path, netloc)
        self.pool = ThreadPoolExecutor(max_workers=10)
        self.scraped = set([])
        self.to_crawl = Queue()

        response = requests.get(self.base_url, timeout=(3, 5))
        soup = BeautifulSoup(markup=response.text, features='html.parser')
        try:
            author = soup.find(name='title').text.split('的主页')[0]
        except Exception:
            author = 'anonymous'
        try:
            max_page = int(soup.find(id='laypage_0').find_all(name='a')[-2].text)
        except Exception:
            max_page = 1

        for i in range(1, max_page + 1):
            url = urljoin(self.base_url, SUFFIX.format(page=i))
            scrapy = Scrapy(type='page', author=author, title=i, url=url)
            if scrapy not in self.scraped:
                self.to_crawl.put(scrapy)

    @staticmethod
    def mkdirs_if_not_exist(dir):
        if not os.path.isdir(dir):
            try:
                os.makedirs(dir)
            except FileExistsError:
                pass

    @staticmethod
    def convert_to_safe_filename(filename):
        return "".join([c for c in filename if c not in '\/:*?"<>|']).strip()

    def scrape_page(self, scrapy):
        try:
            res = requests.get(scrapy.url, timeout=(3, 10))
            return scrapy, res
        except requests.RequestException:
            return

    def parse_topics(self, scrapy, html):
        soup = BeautifulSoup(markup=html.text, features='html.parser')
        for card in soup.find_all(name='a', class_='card-img-hover'):
            scrapy = Scrapy('topic', scrapy.author, card.get('title'), card.get('href'))
            if scrapy not in self.scraped:
                self.to_crawl.put(scrapy)

    def parse_images(self, scrapy, html):
        soup = BeautifulSoup(markup=html.text, features='html.parser')
        for div in soup.find_all(name='div', class_='reveal-work-wrap text-center'):
            url = div.find(name='img').get('src')
            scrapy = Scrapy('image', scrapy.author, scrapy.title, url)
            if scrapy not in self.scraped:
                self.to_crawl.put(scrapy)

    def save_image(self, scrapy, html):
        path = os.path.join(self.save_path,
                            self.convert_to_safe_filename(scrapy.author),
                            self.convert_to_safe_filename(scrapy.title))
        self.mkdirs_if_not_exist(path)
        try:
            name = re.findall(r'(?<=/)\w*?\.jpg|\.png', scrapy.url, re.IGNORECASE)[0]
        except IndexError:
            name = uuid4().hex + '.jpg'
        with open(os.path.join(path, name), 'wb') as fi:
            fi.write(html.content)

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
                target_scrapy = self.to_crawl.get(timeout=10)
                if target_scrapy not in self.scraped:
                    print("Scraping URL: {}".format(target_scrapy))
                    self.scraped.add(target_scrapy)
                    job = self.pool.submit(self.scrape_page, target_scrapy)
                    job.add_done_callback(self.post_scrape_callback)
                    futures.append(job)
            except Empty:
                return
            except Exception as e:
                print(e)
                continue
        wait(futures)


if __name__ == '__main__':
    pass
    s = MultiThreadScraper('https://mixmico.zcool.com.cn/?myCate=0&sort=8&p=2', 'E:\图片')
    s.run_scraper()

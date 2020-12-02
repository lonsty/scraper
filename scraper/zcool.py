# @AUTHOR: lonsty
# @DATE:   2019-09-07 18:34:18
import json
import math
import os.path as op
import re
import sys
import threading
import time
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor, as_completed, wait
from datetime import datetime
from pathlib import Path
from queue import Empty, Queue
from typing import List
from urllib.parse import urljoin, urlparse
from uuid import uuid4

import click
import requests
from bs4 import BeautifulSoup
from pydantic import HttpUrl
from termcolor import colored, cprint

from scraper.utils import (safe_filename, mkdirs_if_not_exist,
                           parse_resources, retry, sort_records)

Scrapy = namedtuple('Scrapy', 'type author title objid index url')  # 用于记录下载任务
HEADERS = {
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'
}
HOST_PAGE = 'https://www.zcool.com.cn'
SEARCH_DESIGNER_SUFFIX = '/search/designer?&word={word}'
USER_SUFFIX = '/u/{id}'
PAGE_SUFFIX = '?myCate=0&sort=1&p={page}'
WORK_SUFFIX = '/work/content/show?p=1&objectId={objid}'
COLLECTION_SUFFIX = '/collection/contents?id={objid}&p={page}&pageSize=25'
USER_API = 'https://www.zcool.com.cn/member/card/{id}'
TIMEOUT = 30
Q_TIMEOUT = 1
MAX_WORKERS = 20
RETRIES = 3

thread_local = threading.local()


def get_session():
    """使线程获取同一个 Session，可减少 TCP 连接数，加速请求。

    :return requests.Session: session
    """
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session


@retry(Exception, tries=RETRIES)
def session_request(url: str, method: str = 'GET') -> requests.Response:
    """使用 session 请求数据。使用了装饰器 retry，在网络异常导致错误时会重试。

    :param str url: 目标请求 URL
    :param str method: 请求方式
    :return requests.Response: 响应数据
    """
    resp = get_session().request(method, url, headers=HEADERS, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp


class ZCoolScraper():

    def __init__(self, user_id=None, username=None, collection=None, destination=None,
                 max_pages=None, spec_topics=None, max_topics=None, max_workers=None,
                 retries=None, redownload=None, overwrite=False, thumbnail=False):
        """初始化下载参数。

        :param int user_id: 用户 ID
        :param str username: 用户名
        :param HttpUrl collection: 收藏集 URL
        :param str destination: 图片保存到本地的路径，默认当前路径
        :param int max_pages: 最大爬取页数，默认所有
        :param list spec_topics: 需要下载的特定主题
        :param int max_topics: 最大下载主题数量，默认所有
        :param int max_workers: 线程开启个数，默认 20
        :param int retries: 请求异常时的重试次数，默认 3
        :param str redownload: 下载记录文件，给定此文件则从失败记录进行下载
        :param bool overwrite: 是否覆盖已存在的文件，默认 False
        :param bool thumbnail: 是否下载缩略图，默认 False
        """
        self.start_time = datetime.now()
        print(f' - - - - - -+-+ {self.start_time.ctime()} +-+- - - - - -\n')
        self.collection = collection
        self.spec_topics = spec_topics
        self.max_topics = max_topics or 'all'
        self.max_workers = max_workers or MAX_WORKERS
        self.pool = ThreadPoolExecutor(self.max_workers)
        self.overwrite = overwrite
        self.thumbnail = thumbnail
        self.pages = Queue()
        self.topics = Queue()
        self.images = Queue()
        self.stat = {
            'npages': 0,
            'ntopics': 0,
            'nimages': 0,
            'pages_pass': set(),
            'pages_fail': set(),
            'topics_pass': set(),
            'topics_fail': set(),
            'images_pass': set(),
            'images_fail': set()
        }

        if retries:
            # 重置全局变量 RETRIES
            global RETRIES
            RETRIES = retries

        dest = Path(destination or '', urlparse(HOST_PAGE).netloc)

        # 从记录文件中的失败项开始下载
        if redownload:
            self.username = self.reload_records(redownload)
            self.user_id = self.search_id_by_username(self.username)
            self.max_pages = self.pages.qsize()
            self.max_topics = self.topics.qsize()
            self.directory = dest / safe_filename(self.username)
            self.stat.update({
                'npages': self.max_pages,
                'ntopics': self.max_topics,
                'nimages': self.images.qsize()
            })
            print(f'{"Username".rjust(17)}: {colored(self.username, "cyan")}\n'
                  f'{"User ID".rjust(17)}: {self.user_id}\n'
                  f'{"Pages to scrapy".rjust(17)}: {self.max_pages:2d}\n'
                  f'{"Topics to scrapy".rjust(17)}: {self.max_topics:3d}\n'
                  f'{"Images to scrapy".rjust(17)}: {self.images.qsize():4d}\n'
                  f'Storage directory: {colored(self.directory, attrs=["underline"])}', end='\n\n')
            self.fetch_all(initialized=True)
            return

        # 从收藏集下载
        if collection:
            objid = self.parse_objid(collection, is_collection=True)
            resp = session_request(urljoin(HOST_PAGE, COLLECTION_SUFFIX.format(objid=objid, page=1)))
            data = resp.json().get('data', {})
            total = data.get('total', 0)
            page_size = data.get('pageable', {}).get('pageSize')
            max_pages_ = math.ceil(total / page_size)
            self.max_pages = min(max_pages or 9999, max_pages_)
            self.directory = dest / safe_filename(f'{self.username}-{self._collection_name}')
            self.parse_collection_topics(data.get('content'))

            # 解析第 2 页 至 最大页的 topic 到下载任务
            for page in range(2, self.max_pages + 1):
                resp = session_request(urljoin(HOST_PAGE, COLLECTION_SUFFIX.format(objid=objid, page=page)))
                self.parse_collection_topics(topics=resp.json().get('data', {}).get('content'),
                                             offset=page_size * (page - 1))

        # 根据用户 ID 或用户名下载
        else:
            self.user_id = user_id or self.search_id_by_username(username)
            self.base_url = urljoin(HOST_PAGE, USER_SUFFIX.format(id=self.user_id))

            try:
                response = session_request(self.base_url)
            except requests.exceptions.ProxyError:
                cprint('Cannot connect to proxy.', 'red')
                sys.exit(1)
            except Exception as e:
                cprint(f'Failed to connect to {self.base_url}, {e}', 'red')
                sys.exit(1)

            soup = BeautifulSoup(markup=response.text, features='html.parser')
            try:
                author = soup.find(name='div', id='body').get('data-name')
                if username and username != author:
                    cprint(f'Invalid user id:「{user_id}」or username:「{username}」!', 'red')
                    sys.exit(1)
                self.username = author
            except Exception:
                self.username = username or 'anonymous'
            self.directory = dest / safe_filename(self.username)
            try:
                max_pages_ = int(soup.find(id='laypage_0').find_all(name='a')[-2].text)
            except Exception:
                max_pages_ = 1
            self.max_pages = min(max_pages or 9999, max_pages_)

        if self.spec_topics:
            topics = ', '.join(self.spec_topics)
        elif self.max_topics == 'all':
            topics = 'all'
        else:
            topics = self.max_pages * self.max_topics
        print(f'{"Username".rjust(17)}: {colored(self.username, "cyan")}\n'
              f'{"User ID".rjust(17)}: {self.user_id}\n'
              f'{"Maximum pages".rjust(17)}: {max_pages_}\n'
              f'{"Pages to scrapy".rjust(17)}: {self.max_pages}\n'
              f'{"Topics to scrapy".rjust(17)}: {topics}\n'
              f'Storage directory: {colored(self.directory, attrs=["underline"])}', end='\n\n')

        self.END_PARSING_TOPICS = False
        self.fetch_all(initialized=True if self.collection else False)

    def search_id_by_username(self, username):
        """通过用户昵称查找用户 ID。

        :param str username: 用户昵称
        :return int: 用户 ID
        """
        if not username:
            cprint('Must give an <user id> or <username>!', 'yellow')
            sys.exit(1)

        search_url = urljoin(HOST_PAGE, SEARCH_DESIGNER_SUFFIX.format(word=username))
        try:
            response = session_request(search_url)
        except requests.exceptions.ProxyError:
            cprint('Cannot connect to proxy.', 'red')
            sys.exit(1)
        except Exception as e:
            cprint(f'Failed to connect to {search_url}, {e}', 'red')
            sys.exit(1)

        author_1st = BeautifulSoup(response.text, 'html.parser').find(name='div', class_='author-info')
        if (not author_1st) or (author_1st.get('data-name') != username):
            cprint(f'Username「{username}」does not exist!', 'yellow')
            sys.exit(1)

        return author_1st.get('data-id')

    def reload_records(self, file):
        """从本地下载记录里读取下载失败的内容。

        :param str file: 下载记录文件的路径。
        :return str: 用户名
        """
        with open(file, 'r', encoding='utf-8') as f:
            for fail in json.loads(f.read()).get('fail'):
                scrapy = Scrapy._make(fail.values())
                if scrapy.type == 'page':
                    self.pages.put(scrapy)
                elif scrapy.type == 'topic':
                    self.topics.put(scrapy)
                elif scrapy.type == 'image':
                    self.images.put(scrapy)
            return scrapy.author

    def generate_pages(self):
        """根据最大下载页数，生成需要爬取主页的任务。"""
        for page in range(1, self.max_pages + 1):
            suffix = COLLECTION_SUFFIX if self.collection else PAGE_SUFFIX
            url = urljoin(self.base_url, suffix.format(page=page))
            scrapy = Scrapy(type='page', author=self.username, title=page,
                            objid=None, index=page - 1, url=url)
            if scrapy not in self.stat["pages_pass"]:
                self.pages.put(scrapy)

    def parse_collection_topics(self, topics: List[dict], offset: int = 0):
        for idx, topic in enumerate(topics):
            new_scrapy = Scrapy(type='topic',
                                author=topic.get('creatorObj', {}).get('username'),
                                title=topic.get('title'),
                                objid=topic.get('id'),
                                index=offset + idx,
                                url=topic.get('pageUrl'))
            if new_scrapy not in self.stat["topics_pass"]:
                self.topics.put(new_scrapy)
                self.stat["ntopics"] += 1

    def parse_topics(self, scrapy):
        """爬取主页，解析所有 topic，并将爬取主题的任务添加到任务队列。

        :param scrapy: 记录任务信息的数据体
        :return Scrapy: 记录任务信息的数据体
        """
        resp = session_request(scrapy.url)
        cards = BeautifulSoup(resp.text, 'html.parser').find_all(name='a', class_='card-img-hover')
        for idx, card in enumerate(cards if self.max_topics == 'all' else cards[:self.max_topics + 1]):
            title = card.get('title')
            if self.spec_topics and (title not in self.spec_topics):
                continue

            new_scrapy = Scrapy(type='topic', author=scrapy.author, title=title,
                                objid=None, index=idx, url=card.get('href'))
            if new_scrapy not in self.stat["topics_pass"]:
                self.topics.put(new_scrapy)
                self.stat["ntopics"] += 1
        return scrapy

    def fetch_topics(self):
        """从任务队列中获取要爬取的主页，使用多线程处理得到需要爬取的主题。"""
        page_futures = {}
        while True:
            try:
                scrapy = self.pages.get(timeout=Q_TIMEOUT)
                page_futures[self.pool.submit(self.parse_topics, scrapy)] = scrapy
            except Empty:
                break
            except Exception:
                continue

        for future in as_completed(page_futures):
            scrapy = page_futures.get(future)
            try:
                future.result()
                self.stat["pages_pass"].add(scrapy)
            except Exception:
                self.stat["pages_fail"].add(scrapy)
                cprint(f'GET page: {scrapy.title} ({scrapy.url}) failed.', 'red')
        self.END_PARSING_TOPICS = True

    def parse_objid(self, url: HttpUrl, is_collection: bool = False) -> str:
        """根据 topic 页面解析 objid

        :param url: topic 或 collection 的 URL
        :return: objid
        """
        soup = BeautifulSoup(session_request(url).text, 'html.parser')
        objid = soup.find('input', id='dataInput').attrs.get('data-objid')
        if is_collection:
            self._collection_name = soup.find('h2', class_='title-h2').text
            user = soup.find(name='span', class_='details-user-avatar')
            self.user_id = user.find('div').attrs.get('data-id')
            self.username = user.find('a').attrs.get('title')
        return objid

    def parse_images(self, scrapy):
        """爬取 topic，获得 objid 后直接调用 API，从返回数据里获得图片地址等信息，

        并将下载图片的任务添加到任务队列。
        :param scrapy: 记录任务信息的数据体
        :return Scrapy: 记录任务信息的数据体
        """
        objid = scrapy.objid or self.parse_objid(scrapy.url)
        resp = session_request(urljoin(HOST_PAGE, WORK_SUFFIX.format(objid=objid)))
        data = resp.json().get('data', {})
        author = data.get('product', {}).get('creatorObj', {}).get('username')
        title = data.get('product', {}).get('title')
        objid = data.get('product', {}).get('id')

        for img in data.get('allImageList', []):
            new_scrapy = Scrapy(type='image', author=author, title=title,
                                objid=objid, index=img.get('orderNo') or 0, url=img.get('url'))
            if new_scrapy not in self.stat["images_pass"]:
                self.images.put(new_scrapy)
                self.stat["nimages"] += 1
        return scrapy

    def fetch_images(self):
        """从任务队列中获取要爬取的主题，使用多线程处理得到需要下载的图片。"""
        image_futures = {}
        while True:
            try:
                scrapy = self.topics.get(timeout=Q_TIMEOUT)
                image_futures[self.pool.submit(self.parse_images, scrapy)] = scrapy
            except Empty:
                if self.END_PARSING_TOPICS:
                    break
            except Exception:
                continue

        for future in as_completed(image_futures):
            scrapy = image_futures.get(future)
            try:
                future.result()
                self.stat["topics_pass"].add(scrapy)
            except Exception:
                self.stat["topics_fail"].add(scrapy)
                cprint(f'GET topic: {scrapy.title} ({scrapy.url}) failed.', 'red')

    def fetch_all(self, initialized: bool = False):
        """同时爬取主页、主题，并更新状态。"""
        if not initialized:
            self.generate_pages()
        fetch_futures = [self.pool.submit(self.fetch_topics),
                         self.pool.submit(self.fetch_images)]
        end_show_fetch = False
        t = threading.Thread(target=self.show_fetch_status, kwargs={'end': lambda: end_show_fetch})
        t.start()
        try:
            wait(fetch_futures)
        except KeyboardInterrupt:
            raise
        finally:
            end_show_fetch = True
            t.join()

    def show_fetch_status(self, interval=0.5, end=None):
        """用于后台线程，实现边爬取边显示状态。

        :param int interval: 状态更新间隔，秒
        :param function end: 用于控制退出线程
        """
        while True:
            status = 'Fetched Pages: {pages}\tTopics: {topics}\tImages: {images}'.format(
                pages=colored(str(self.max_pages).rjust(3), 'blue'),
                topics=colored(str(self.stat["ntopics"]).rjust(3), 'blue'),
                images=colored(str(self.stat["nimages"]).rjust(5), 'blue'))
            print(status, end='\r', flush=True)
            if (interval == 0) or (end and end()):
                print('\n')
                break
            time.sleep(interval)

    def show_download_status(self, interval=0.5, end=None):
        """用于后台线程，实现边下载边显示状态。

        :param int interval: 状态更新间隔，秒
        :param function end: 用于控制退出线程
        """
        while True:
            completed = len(self.stat["images_pass"]) + len(self.stat["images_fail"])
            if self.stat["nimages"] > 0:
                status = 'Time used: {time_used}\tFailed: {failed}\tCompleted: {completed}'.format(
                    time_used=colored(str(datetime.now() - self.start_time)[:-7], 'yellow'),
                    failed=colored(str(len(self.stat["images_fail"])).rjust(3), 'red'),
                    completed=colored(str(int(completed / self.stat["nimages"] * 100))
                                      + f'% ({completed}/{self.stat["nimages"]})', 'green'))
                print(status, end='\r', flush=True)
            if (interval == 0) or (end and end()):
                if self.stat["nimages"] > 0:
                    print('\n')
                break
            time.sleep(interval)

    def download_image(self, scrapy):
        """下载图片保存到本地。

         :param scrapy: 记录任务信息的数据体
         :return Scrapy: 记录任务信息的数据体
         """
        try:
            name = re.findall(r'(?<=/)\w*?\.(?:jpg|gif|png|bmp)', scrapy.url, re.IGNORECASE)[0]
        except IndexError:
            name = uuid4().hex + '.jpg'

        path = self.directory / safe_filename(scrapy.title)
        filename = path / f'[{scrapy.index + 1 or 0:02d}]{name}'
        if (not self.overwrite) and op.isfile(filename):
            return scrapy

        url = scrapy.url
        if self.thumbnail:
            if url.lower().endswith(('jpg', 'png', 'bmp')):
                url = f'{scrapy.url}@1280w_1l_2o_100sh.{url[-3:]}'
        resp = session_request(url)

        mkdirs_if_not_exist(path)
        with open(filename, 'wb') as f:
            for chunk in resp.iter_content(8192):
                f.write(chunk)
        return scrapy

    def save_records(self):
        """将成功及失败的下载记录保存到本地文件。

        :return str: 记录文件的路径
        """
        filename = f'{safe_filename(self.start_time.isoformat()[:-7])}.json'
        abspath = op.abspath(self.directory / filename)
        with open(abspath, 'w', encoding='utf-8') as f:
            success = (self.stat["pages_pass"] | self.stat["topics_pass"] | self.stat["images_pass"])
            fail = (self.stat["pages_fail"] | self.stat["topics_fail"] | self.stat["images_fail"])
            type_order = {'page': 1, 'topic': 2, 'image': 3}
            s_ordered = sort_records(success, order=type_order)
            f_ordered = sort_records(fail, order=type_order)

            records = {
                'time': self.start_time.isoformat(),
                'success': [scrapy._asdict() for scrapy in s_ordered],
                'fail': [scrapy._asdict() for scrapy in f_ordered]
            }
            f.write(json.dumps(records, ensure_ascii=False, indent=2))
        return abspath

    def run_scraper(self):
        """使用多线程下载所有图片，完成后保存记录并退出程序。"""
        end_show_download = False
        t = threading.Thread(target=self.show_download_status, kwargs={'end': lambda: end_show_download})
        t.start()

        image_futuress = {}
        while True:
            try:
                scrapy = self.images.get_nowait()
                if scrapy not in self.stat["images_pass"]:
                    image_futuress[self.pool.submit(self.download_image, scrapy)] = scrapy
            except Empty:
                break
            except KeyboardInterrupt:
                raise
            except Exception:
                continue

        try:
            for future in as_completed(image_futuress):
                scrapy = image_futuress.get(future)
                try:
                    future.result()
                    self.stat["images_pass"].add(scrapy)
                except Exception:
                    self.stat["images_fail"].add(scrapy)
                    cprint(f'Download image: {scrapy.title}[{scrapy.index + 1}] '
                           f'({scrapy.url}) failed.', 'red')
        except KeyboardInterrupt:
            raise
        finally:
            end_show_download = True
            t.join()

        saved_images = len(self.stat["images_pass"])
        failed_images = len(self.stat["images_fail"])
        if saved_images or failed_images:
            if saved_images:
                print(f'Saved {colored(saved_images, "green")} images to '
                      f'{colored(self.directory.absolute(), attrs=["underline"])}')
            records_path = self.save_records()
            print(f'Saved records to {colored(records_path, attrs=["underline"])}')
        else:
            cprint('No images to download.', 'yellow')


@click.command()
@click.option('-u', '--usernames', 'names', help='One or more user names, separated by commas.')
@click.option('-i', '--ids', 'ids', help='One or more user IDs, separated by commas.')
@click.option('-c', '--collections', 'collections', help='One or more collection URLs, separated by commas.')
@click.option('-t', '--topics', 'topics', help='Specific topics to download, separated by commas.')
@click.option('-d', '--destination', 'destination', help='Destination to save images.')
@click.option('-R', '--retries', 'retries', default=RETRIES, show_default=True, type=int,
              help='Repeat download for failed images.')
@click.option('-r', '--redownload', 'redownload',
              help='Redownload images from failed records (PATH of the .json file).')
@click.option('-o', '--overwrite', 'overwrite', is_flag=True, default=False, help='Override the existing files.')
@click.option('--thumbnail', 'thumbnail', is_flag=True, default=False,
              help='Download thumbnails with a maximum width of 1280px.')
@click.option('--max-pages', 'max_pages', type=int, help='Maximum pages to download.')
@click.option('--max-topics', 'max_topics', type=int, help='Maximum topics per page to download.')
@click.option('--max-workers', 'max_workers', default=MAX_WORKERS, show_default=True, type=int,
              help='Maximum thread workers.')
def zcool_command(ids, names, collections, destination, max_pages, topics, max_topics,
                  max_workers, retries, redownload, overwrite, thumbnail):
    """ZCool picture crawler, download pictures, photos and illustrations of
    ZCool (https://zcool.com.cn/). Visit https://github.com/lonsty/scraper.
    """
    if redownload:
        scraper = ZCoolScraper(destination=destination, max_pages=max_pages, spec_topics=topics,
                               max_topics=max_topics, max_workers=max_workers, retries=retries,
                               redownload=redownload, overwrite=overwrite, thumbnail=thumbnail)
        scraper.run_scraper()

    elif any([ids, names, collections]):
        topics = topics.split(',') if topics else []
        resources = parse_resources(ids, names, collections)
        for res in resources:
            scraper = ZCoolScraper(user_id=res.id, username=res.name, collection=res.collection,
                                   destination=destination, max_pages=max_pages, spec_topics=topics,
                                   max_topics=max_topics, max_workers=max_workers, retries=retries,
                                   redownload=redownload, overwrite=overwrite)
            scraper.run_scraper()

    else:
        click.echo('Try "python crawler.py --help" for help.')
        return 1
    return 0

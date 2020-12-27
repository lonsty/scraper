# @FILENAME : test_requests
# @AUTHOR : lonsty
# @DATE : 2020/12/27 14:44
import os
import threading
from concurrent.futures import ThreadPoolExecutor, wait
from pathlib import Path
import traceback

import requests

# http://www.cnu.cc/works/427334
# urls = [
#     'http://imgoss.cnu.cc/2010/30/994dfe50c509344eb3f5b525df642d60.jpg',
#     'http://imgoss.cnu.cc/2010/30/4e695801d1c83c559991017cb8e2ff7a.jpg',
#     'http://imgoss.cnu.cc/2010/30/07c3089cf6383bf596c59e92dc105412.jpg',
#     'http://imgoss.cnu.cc/2010/30/a5b738e0f21737bb953e7407c8425772.jpg',
#     'http://imgoss.cnu.cc/2010/30/24dd6e27c9cf38eab7daa72cc3e182e3.jpg',
#     'http://imgoss.cnu.cc/2010/30/624cbd2e69313174b6ad13a9d2e75279.jpg',
#     'http://imgoss.cnu.cc/2010/30/9786475b74733c91ba7f8b638468e299.jpg',
#     'http://imgoss.cnu.cc/2010/30/75e085d6573c326dab3a8959736cb355.jpg',
#     'http://imgoss.cnu.cc/2010/30/d28d0a7729353414a58e8fa10ff11fe8.jpg',
#     'http://imgoss.cnu.cc/2010/30/669be1298a3e3546974977ed7c9655eb.jpg',
#     'http://imgoss.cnu.cc/2010/30/ee82358fd6a537108d938ebba7cfe7de.jpg',
#     'http://imgoss.cnu.cc/2010/30/007ce903478e3f6aa867a5789d213748.jpg',
#     'http://imgoss.cnu.cc/2010/30/49549ca620873ba5bf71dd8c92d9e006.jpg',
#     'http://imgoss.cnu.cc/2010/30/11426afdb0453d1b86bc7a2bb187bcef.jpg',
#     'http://imgoss.cnu.cc/2010/30/39b4116ba53731f3994acddef431532c.jpg',
#     'http://imgoss.cnu.cc/2010/30/7a7b6ae21eb13701a8f238aaedcf6d7d.jpg',
#     'http://imgoss.cnu.cc/2010/30/e9ed54e368873d44b6b8fc276f0a6018.jpg',
#     'http://imgoss.cnu.cc/2010/30/5f97c9f5ff6f353f814be1f9867ba6d0.jpg',
#     'http://imgoss.cnu.cc/2010/30/42fb0868ca8434ec8c5f3460b96b2b5b.jpg',
#     'http://imgoss.cnu.cc/2010/30/584e84b9e0253c80ac33f22a33adb9df.jpg'
# ]

# http://www.cnu.cc/works/435640
urls = [
    'http://imgoss.cnu.cc/2012/25/pv6kqgjspuf4e9mefu01608863905044.jpg',
    'http://imgoss.cnu.cc/2012/25/tpl70xav4fk8zsu55881608863905046.jpg',
    'http://imgoss.cnu.cc/2012/25/9mkp309it3ub5qv9f6f1608863905046.jpg',
    'http://imgoss.cnu.cc/2012/25/zb7f9qlpu75x2s53i821608863905047.jpg',
    'http://imgoss.cnu.cc/2012/25/r2kxv11qltnruneqlpk1608863905047.jpg',
    'http://imgoss.cnu.cc/2012/25/oqr644pxdcxeb404n1e1608863905048.jpg',
    'http://imgoss.cnu.cc/2012/25/vjgt0am668sus1kvvcj1608863905048.jpg',
    'http://imgoss.cnu.cc/2012/25/wjchu6v3en8iin2x3qf1608863905049.jpg',
    'http://imgoss.cnu.cc/2012/25/jnq983zvv9k6iatdofo1608863905049.jpg'
]
thread_local = threading.local()
dest = Path('www.cnu.cc/冬日暖阳')


def mkdirs_if_not_exist(dir):
    """文件夹不存在时则创建。

    :param str dir: 文件夹路径，支持多级
    """
    if not os.path.isdir(dir):
        try:
            os.makedirs(dir)
            return True
        except FileExistsError:
            pass


def get_session():
    """使线程获取同一个 Session，可减少 TCP 连接数，加速请求。

    :return requests.Session: session
    """
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.Session()
    return thread_local.session


def download_image(url):
    print(f'Downloading {url} ...')
    session = get_session()
    try:
        response = session.get(url, timeout=20)
    except Exception:
        print(traceback.format_exc())
        return
    filepath = dest / url.split("/")[-1]
    with open(filepath, 'wb') as f:
        for chunk in response.iter_content(8192):
            f.write(chunk)
        print(f'Saved to {filepath}')


if __name__ == '__main__':
    print('Start ...')
    os.makedirs(dest, exist_ok=True)
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = [pool.submit(download_image, url) for url in urls]
    wait(futures)
    print('Done.')

# @FILENAME : utils
# @AUTHOR : lonsty
# @DATE : 2019/9/9 11:09
import os
import random
import time
from collections import namedtuple
from functools import wraps


def retry(exceptions, tries=3, delay=1, backoff=2, logger=None):
    """Retry calling the decorated function using an exponential backoff.

    :param exceptions: The exception to check. may be a tuple of exceptions to check.
    :param tries: Number of times to try (not retry) before giving up.
    :param delay: Initial delay between retries in seconds.
    :param backoff: Backoff multiplier (e.g. value of 2 will double the delay each retry).
    :param logger: Logger to use. If None, print.
    """

    def deco_retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay or random.uniform(0.5, 1.5)
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except exceptions as e:
                    if logger:
                        logger.warning('{}, Retrying in {} seconds...'.format(e, mdelay))
                    # else:
                    #     print('{}, Retrying in {} seconds...'.format(e, mdelay))
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)

        return f_retry  # true decorator

    return deco_retry


def mkdirs_if_not_exist(dir):
    """文件夹不存在时则创建。

    :param str dir: 文件夹路径，支持多级
    """
    if not os.path.isdir(dir):
        try:
            os.makedirs(dir)
        except FileExistsError:
            pass


def convert_to_safe_filename(filename):
    """去掉文件名中的非法字符。

    :param str filename: 文件名
    :return str: 合法文件名
    """
    return "".join([c for c in filename if c not in r'\/:*?"<>|']).strip()


def parse_users(ids, names):
    """解析用户名或 ID。

    :param str ids: 半角逗号分隔的用户 ID
    :param str names: 半角逗号分隔的用户名
    :return list: 包含 User 数据的列表
    """
    User = namedtuple('User', 'id name')
    users = []

    if names:
        users = [User(None, name) for name in names.split(',')]
    elif ids:
        users = [User(uid, None) for uid in ids.split(',')]

    return users

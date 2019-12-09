# @FILENAME : utils
# @AUTHOR : lonsty
# @DATE : 2019/9/9 11:09
import os
import random
import time
from collections import namedtuple
from functools import wraps


def retry(exceptions, tries=3, delay=1, backoff=2, logger=None):
    """
    Retry calling the decorated function using an exponential backoff.
    Args:
        exceptions: The exception to check. may be a tuple of
            exceptions to check.
        tries: Number of times to try (not retry) before giving up.
        delay: Initial delay between retries in seconds.
        backoff: Backoff multiplier (e.g. value of 2 will double the delay
            each retry).
        logger: Logger to use. If None, print.
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
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)

        return f_retry  # true decorator

    return deco_retry


def mkdirs_if_not_exist(dir):
    if not os.path.isdir(dir):
        try:
            os.makedirs(dir)
        except FileExistsError:
            pass


def convert_to_safe_filename(filename):
    return "".join([c for c in filename if c not in r'\/:*?"<>|']).strip()


def parse_users(ids, names):
    User = namedtuple('User', 'id name')
    users = []

    if names:
        users = [User('', name) for name in names.split(',')]
    elif ids:
        users = [User(uid, '') for uid in ids.split(',')]

    return users

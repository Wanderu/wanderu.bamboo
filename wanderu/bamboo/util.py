# Copyright 2015 Wanderu, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Py 3 Compatibility
from __future__ import (absolute_import, division, print_function, unicode_literals)

import uuid
from calendar import timegm
from datetime import datetime
from itertools import chain
from os import getpid
from socket import gethostname
from string import ascii_lowercase, digits
from random import choice
from uuid import uuid1
from urlparse import urlparse
from urllib import unquote
from wanderu.bamboo.config import RE_HASHSLOT

MS_TO_SEC = 10**6

def makeClusterNamespace(namespace):
    """ref: Redis Cluster key hashing for slot selection.

    >>> makeClusterNamespace("HELLO:WORLD") == "{HELLO:WORLD}"
    True
    >>> makeClusterNamespace("{HELLO:WORLD}") == "{HELLO:WORLD}"
    True
    >>> makeClusterNamespace("{HELLO}:WORLD") == "{HELLO}:WORLD"
    True
    >>> makeClusterNamespace("HELLO:{WORLD}") == "HELLO:{WORLD}"
    True
    """
    return RE_HASHSLOT.match(namespace) and namespace \
                        or ("{%s}" % namespace)

def parse_url(url):
    """url: redis://localhost:6379/0"""
    pr = urlparse(url)
    host, path, password = map(unquote,
            (pr.hostname or "", pr.path or "", pr.password or ""))
    dbid = path[1:].split('/')[0]
    return (host or "localhost", pr.port or 6379, dbid or 0,
            password or None)

def twos(l):
    """
    >>> list(twos(['a', 'b'])) == [('a', 'b')]
    True
    >>> list(twos(['a'])) == []
    True
    >>> list(twos(['a', 'b', 'c', 'd'])) == [('a', 'b'), ('c', 'd')]
    True
    >>> list(twos(['a', 'b', 'c', 'd', 'e'])) == [('a', 'b'), ('c', 'd')]
    True
    """
    it = iter(l)
    while True:
        yield next(it), next(it)

def make_key(ns_sep, namespace, *names):
    """Make a redis namespaced key.

    >>> make_key(":", "YOO:HOO", "a", "b", "c") == "YOO:HOO:a:b:c"
    True
    """
    return ns_sep.join(chain((namespace,), names))

def utcunixts(dt=None):
    if dt is None:
        dt = datetime.utcnow()
    nowunixtz = timegm(dt.utctimetuple())
    nowunixtzms = (nowunixtz + float(dt.microsecond)/MS_TO_SEC)
    return nowunixtzms


def random_chars(n=1):
    return "".join([choice(list(chain(ascii_lowercase, digits)))
                    for _ in xrange(n)])


def gen_random_name():
    return str(uuid1())  # or use uuid4


def name_for_host():
    return "-".join((gethostname().lower(),
                     str(getpid()),
                     random_chars(6)))

def gen_worker_name():
    # try:
        # gethostname can fail in some contexts
    return name_for_host()
    # except:
    #     return gen_random_name()

def unique_job_id():
    return uuid.uuid1().hex

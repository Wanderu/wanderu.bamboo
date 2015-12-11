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

from calendar import timegm
from datetime import datetime
from itertools import chain
from os import getpid
from socket import gethostname
from string import ascii_lowercase, digits
from random import choice

MS_TO_SEC = 10**6

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


def unique_name():
    return "_".join((gethostname().lower(),
                     str(getpid()),
                     random_chars(6)))

from itertools import chain
from datetime import datetime
from calendar import timegm

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

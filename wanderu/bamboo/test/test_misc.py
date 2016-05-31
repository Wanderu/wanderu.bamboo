# Py 3 Compatibility
from __future__ import (absolute_import, division, print_function, unicode_literals)

from wanderu.bamboo.config import RE_HASHSLOT
from wanderu.bamboo.util import gen_random_name, unique_job_id

def test_re_hashslot():
    assert(RE_HASHSLOT.match("{}aaabbbccc") != None)
    assert(RE_HASHSLOT.match("{aaabbbccc}") != None)
    assert(RE_HASHSLOT.match("aaa{bbbccc}") != None)
    assert(RE_HASHSLOT.match("{aaabbb}ccc") != None)
    assert(RE_HASHSLOT.match("aaa{bbb}ccc") != None)
    assert(RE_HASHSLOT.match("aaa{bbbccc")  == None)
    assert(RE_HASHSLOT.match("aaabbb}ccc")  == None)
    assert(RE_HASHSLOT.match("{aaabbbccc")  == None)
    assert(RE_HASHSLOT.match("aaabbbccc}")  == None)

def test_gen_random_name():
    name = gen_random_name()
    assert len(name) > 0

def test_unique_job_id():
    jid = unique_job_id()
    assert len(jid) > 0

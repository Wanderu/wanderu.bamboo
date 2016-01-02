from wanderu.bamboo.config import RE_HASHSLOT

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

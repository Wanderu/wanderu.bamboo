# coding: utf-8

import codecs
from pkg_resources import resource_filename, resource_listdir
from string import Template
from os.path import isdir, join as pathjoin

from wanderu.bamboo.config import (LUA_EXT, LUA_SCR_PKG, LUA_SCR_DIR)

def read_resource(package, resource, enc='utf-8'):
    filename = resource_filename(package, resource)
    with codecs.open(filename, encoding=enc) as fp:
        return fp.read()

def read_lua_scripts(filenames):
    """
    Retrieve the contents of lua scripts and return a map of the script
    basename to its contents.
    """
    return {
        filename[:-len(LUA_EXT)]:
            read_resource(LUA_SCR_PKG, pathjoin(LUA_SCR_DIR, filename))
        for filename in filenames
    }

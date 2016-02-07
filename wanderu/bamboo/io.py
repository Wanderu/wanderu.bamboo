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
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

import codecs
from pkg_resources import resource_filename
from os.path import join as pathjoin

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

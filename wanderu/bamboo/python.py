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

from wanderu.bamboo.util import makeClusterNamespace

# This is a generic way to create really small instances that are views of
# other instances. We can create 1 (Tx)RedisJobQueue object, which inclueds
# it's redis connection pool, scripts, etc. and reuse it for different
# namespaces. This cuts down on memory usage and active connections.

VIEW_CLASSES = {}

def viewGetAttr(self, attr):
    """The view's __getattr__ method delegates failed lookups
    to the proxy object ("_subject").
    """
    return getattr(self._subject, attr)

def viewInit(self, subject, namespace):
    if getattr(subject, "_subject", None) is not None:
        raise Exception("_subject was specified on the subclass and should not be.")
    self._subject = subject
    self.namespace = namespace

def getNamespaceViewForQueue(subject, namespace):
    """Dynamically create a new View class that subclasses the proxy object
    so that all propery/method lookups happen on the correct object.
    `namespace` will be found in this new View class and all other attributes
    will be found on the `subject` instance.
    """
    namespace = makeClusterNamespace(namespace)
    view_name = str("".join((type(subject).__name__, "View")))
    if view_name not in VIEW_CLASSES:
        cls = type(view_name, (type(subject),),
                   {"__slots__": ["namespace", "_subject"],
                    "__getattr__": viewGetAttr,
                    "__init__": viewInit})
        VIEW_CLASSES[view_name] = cls
    inst = VIEW_CLASSES[view_name](subject, namespace)
    return inst

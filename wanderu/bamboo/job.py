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

from itertools import chain
import collections

import six

from wanderu.bamboo.util import twos, utcunixts
from wanderu.bamboo.config import DEFAULT_PRIORITY

@six.python_2_unicode_compatible
class GenericModel(object):
    """
    Defining the fields allows us to automatically convert all string-based
    parameters passed to expected types.

    _fields structure:
        A dict of:
            <key>: <meta_dict>
        Where:
        <key> is a string
        <meta_dict> is a dict containing attribute information describing the
                    value stored in <key>.
                    Valid attributes include:
                        'type': a callable Python type
                        'default: a value or callable function that returns a
                                  default value
    """

    _fields = dict()

    def __init__(self, **kwargs):
        for name, meta in self._fields.items():
            # set default values for items on initialization
            default = meta.get('default', None)
            setattr(self, name, kwargs.get(name, default() if isinstance(default, collections.Callable) else default))

    def __setattr__(self, k, v):
        """Make sure it is a valid field and convert to expected type if
        specified."""
        if k in self._fields:
            object.__setattr__(self, k,
                        self._fields[k]['type'](v)
                        if 'type' in self._fields[k] and
                        v is not None else v)
        else:
            raise KeyError("Invalid field: %s" % k)

    @classmethod
    def from_string_list(cls, l):
        return cls(**{six.text_type(k): v for k, v in twos(l)})

    def as_string_tup(self, filter=True):
        # tuple(chain(*job.as_dict(filter=True).items()))
        return tuple(chain(*((k, getattr(self, k)) for k in self._fields
                            if not filter or getattr(self, k, None))))

    @classmethod
    def from_dict(cls, d):
        return cls(**d)

    def as_dict(self, filter=False):
        """Return a dictionary representation of the Model object.
        If `filter` is True, returns a dictionary of only attributes that
        are not None (have been set).
        Assumption: If an argument has been set, it is not None.
        """
        return {k: getattr(self, k) for k in self._fields
                    if not filter or getattr(self, k, None)}

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.as_dict() == other.as_dict()

    def __ne__(self, other):
        return (not self.__eq__(other))

    def __str__(self):
        return "{klass}({params})".format(
                klass=self.__class__.__name__,
                params= ", ".join(("{k}={v}".format(k=k, v=six.text_type(v))
                                   for k, v in sorted(self.as_dict().items())
                                   if v is not None))
                )

    def __repr__(self):
        """ob == eval(repr(ob))"""
        # __repr__() must return a str on all versions of Python.
        return "{klass}({params})".format(
                klass=self.__class__.__name__,
                params= ", ".join(("{k}={v}".format(k=k, v=repr(v))
                                   for k, v in sorted(self.as_dict().items())))
                )

    def __hash__(self):
        return hash(repr(self))

def text_type_utf8(s):
    if isinstance(s, six.binary_type):
        return six.text_type(s, encoding='utf-8')
    return s

class Job(GenericModel):
    """
    Enquing information:
        id:       Unique ID for this job (unique b/c it is the set key)
        priority: integer. (lower means higher priority)
        payload:  string. Processing details for worker.
        created:  integer. Unix UTC timestamp.
        state:    Value from `config.JOB_STATES`
    Post-consume:
        owner:    name of client that has consumed this job
        consumed: integer. Unix UTC timestamp.
        failures: integer. Number of failures.
        failed:   integer. Unix UTC timestamp.
    """

    _fields = {
        'id'         : {'type': text_type_utf8},
        'priority'   : {'type': float, 'default': DEFAULT_PRIORITY},
        'payload'    : {}, # used to be 'parameters'
        'created'    : {'type': float,
                        'default':  utcunixts},
        'failures'   : {'type': int, 'default': 0},
        'failed'     : {'type': float},
        'consumed'   : {'type': float},
        'owner'      : {'type': text_type_utf8},
        'contenttype': {'type': text_type_utf8, 'default': ""},
        'encoding'   : {'type': text_type_utf8, 'default': ""},
        'state'      : {'type': text_type_utf8},
    }


    # NOTE: Redis does not allow us to load the os module by default and that
    # is the module that can get the system time.
    # Therefore, we have to set an object creation time here and pass
    # it through.

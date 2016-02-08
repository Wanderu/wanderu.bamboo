# Py 3 Compatibility
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from twisted.internet import defer
import txredisapi as redis

class Script(object):
    """A script object that maintains most of the interface from the synchronous
    redis library (`redis.client.Script`), which includes the attributes
    `registered_client`, `script`, and `sha`, as well as being a callable class.
    """
    def __init__(self, registered_client, script):
        self.registered_client = registered_client
        self.script = script
        self.sha = ""

    def __call__(self, *args, **kwargs):
        """Present a callable interface like the syncronous redis library."""
        return self.eval(*args, **kwargs)

    def _script_load_success(self, sha):
        self.sha = sha
        return sha

    def _load_script(self):
        # returns a deferred that returns the script hash
        d = self.registered_client.script_load(self.script)
        d.addCallback(self._script_load_success)
        return d

    def _eval_failure(self, failure, keys, args):
        if failure.check(redis.ScriptDoesNotExist) is not None:
            # reload script
            d = self._load_script()
            # next callback gets the sha
            d.addCallback(self._eval, keys, args)
            return d

        # continue the failure
        return failure

    def _eval(self, sha, keys, args):
        d = self.registered_client.evalsha(sha, keys, args)
        d.addErrback(self._eval_failure, keys, args)
        return d

    def eval(self, keys=[], args=[]):
        if self.sha is None:
            d = self._load_script()
        else:
            d = defer.succeed(self.sha)

        d.addCallback(self._eval, keys, args)
        return d

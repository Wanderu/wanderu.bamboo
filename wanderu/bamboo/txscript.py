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
        self.sha = None

    def _script_load_success(self, sha):
        self.sha = sha
        return sha

    def _load_script(self):
        """
        Returns a deferred that is called back with the script sha
        """
        return self.registered_client.script_load(self.script)\
                   .addCallback(self._script_load_success)

    def _eval_failure(self, failure, keys, args):
        # If redis doesn't know about the script, then re-add it.
        # This can happen during a service restart, for example.
        if failure.check(redis.ScriptDoesNotExist) is not None:
            return self._load_script().addCallback(self._eval, keys, args)

        # Otherwise continue the failure
        return failure

    def _eval(self, sha, keys, args):
        d = self.registered_client.evalsha(sha, keys, args)
        d.addErrback(self._eval_failure, keys, args)
        return d

    def __call__(self, keys=[], args=[]):
        """
        Present a callable interface like the synchronous redis library
        """
        # A deferred that is called back with the script sha
        d = self._load_script() if self.sha is None else defer.succeed(self.sha)
        d.addCallback(self._eval, keys, args)
        return d

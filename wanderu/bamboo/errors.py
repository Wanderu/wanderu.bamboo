# Py 3 Compatibility
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
import re

# An regular expression that captures the error code from a Lua script's return
# value. We expect all Lua script error messages to start with an error code.
# Expected error codes are defined below.
RE_ERROR_CODE = re.compile(
    r'([_A-Za-z]+)'  # "_" and a/A through z/Z characters that we want to capture
    r'[:\S-]?'       # An optional ":", whitespace, or "-"
    r'.*'            # Anything else that we don't care about
)

class OperationError(Exception):
    """Used when an operation fails or cannot be executed.
    """
    pass

# Normal errors
class NormalOperationError(OperationError):
    """Normal Operation Errors are those that are expected errors during
    normal use of the system. They are used as control sequences.
    """

class NoItems(NormalOperationError):
    pass

class MaxJobsReached(NormalOperationError):
    pass

# Abnormal errors
class AbnormalOperationError(OperationError):
    """Abnormal Operaion Errors are those where under normal circumstances,
    they should not be raised if the program is using the system/library correctly.
    """

class JobInWork(AbnormalOperationError):
    """An operation was requested to be performed on a Job that is currently
    actively being worked and the operation cannot complete.
    """

class JobExists(AbnormalOperationError):
    """The Job that was asked to be scheduled is already in the system.
    """

class UnknownJobId(AbnormalOperationError):
    """A job ID was specified, but the system does not know about it.
    """

class InvalidParameter(AbnormalOperationError):
    pass

class InvalidClientName(AbnormalOperationError):
    pass

# A mapping from the Lua script error reply codes to exception classes.
SCRIPT_ERROR_CODES_TO_EXCEPTIONS = {
    # Normal
    'JOB_IN_WORK': JobInWork,
    'NO_ITEMS': NoItems,
    'MAXJOBS_REACHED': MaxJobsReached,
    # Abnormal
    'INVALID_PARAMETER': InvalidParameter,
    'JOB_EXISTS': JobExists,
    'UNKNOWN_JOB_ID': UnknownJobId,
    'INVALID_CLIENT_NAME': InvalidClientName
}

def message_to_error(msg):
    """
    >>> isinstance(message_to_error("NO_ITEMS"), NoItems)
    True
    >>> isinstance(message_to_error("NO_ITEMS:"), NoItems)
    True
    >>> isinstance(message_to_error("NO_ITEMS: Some more message"), NoItems)
    True
    >>> isinstance(message_to_error("Some other message"), OperationError)
    True
    """
    match = RE_ERROR_CODE.match(msg)
    if match is not None:
        if match.group(1) in SCRIPT_ERROR_CODES_TO_EXCEPTIONS:
            return SCRIPT_ERROR_CODES_TO_EXCEPTIONS[match.group(1)](msg)
    return OperationError(msg)

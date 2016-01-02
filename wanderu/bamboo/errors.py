import re
RE_ERR = re.compile(r'([_A-Za-z]+)[:\S-]?.*')

class OperationError(Exception):
    """Used when an operation fails or cannot be executed.
    """
    pass

class UnknownJobId(OperationError):
    pass

class JobInWork(OperationError):
    pass

class InvalidParameter(OperationError):
    pass

class JobExists(OperationError):
    pass

class MaxJobsReached(OperationError):
    pass

class InvalidClientName(OperationError):
    pass

class NoItems(OperationError):
    pass

KNOWN_ERRORS = {
    'UNKNOWN_JOB_ID': UnknownJobId,
    'JOB_IN_WORK': JobInWork,
    'INVALID_PARAMETER': InvalidParameter,
    'JOB_EXISTS': JobExists,
    'MAXJOBS_REACHED': MaxJobsReached,
    'INVALID_CLIENT_NAME': InvalidClientName,
    'NO_ITEMS': NoItems
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
    match = RE_ERR.match(msg)
    if match is not None:
        if match.group(1) in KNOWN_ERRORS:
            return KNOWN_ERRORS[match.group(1)](msg)
    return OperationError(msg)

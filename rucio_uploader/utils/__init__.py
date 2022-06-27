"""@package utils

 Some utils functions

"""

import os
import hashlib
from datetime import datetime

def format_now() -> str:
    """Return a string with formatted datetime

    Returns:
        str: string with formatted datetime
    """
    return "[{}]".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

def format_log_message(message: str, function) -> str:
    """Return formatted log message

    Args:
        message (str): text message to be formatted
        function (function): function logging the message

    Returns:
        str: formatted log message
    """
    return "   {} : [{}] -> \"{}\"\n".format(format_now(), function.__name__, message)

def get_scoped_name(name: str, scope: str) -> str:
    """Return a string in the form: "<scope>:<name>"

    Args:
        name (str): filename
        scope (str): scope

    Returns:
        str: a string in the form: "<scope>:<name>"]
    """
    return "{}:{}".format(scope,name)

def get_scope_and_name(sname: str) -> str:
    """Split the scoped name and return a tuple with scope and name

    Args:
        sname (str): scoped name

    Returns:
        str: a tuple with scope and name
    """
    splits = sname.split(':')
    return splits[0],splits[1]

def sources_exist(sources: list) -> bool:
    """Check the sources exist

    Args:
        sources (list): list of sources

    Returns:
        bool: True if all sources exist, False otherwise
    """
    if sources is not None:
        for s in sources:
            if not os.path.exists(s):
                print("Error: {} does not exits".format(s))
                return False
    return True

def md5(fname):
    """Evaluate md5 checksum of a file

    Args:
        fname (str): path of a file

    Returns:
        str: checksum of a file
    """
    if os.path.getsize(fname) == 0:
        return "0"
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

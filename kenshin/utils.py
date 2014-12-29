# coding: utf-8

import os
import math
import errno

from consts import NULL_VALUE


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def roundup(x, base):
    """
    Roundup to nearest multiple of `base`.

    >>> roundup(21, 10)
    30
    >>> roundup(20, 10)
    20
    >>> roundup(19, 10)
    20
    """
    t = x % base
    return (x - t + base) if t else x


def is_null_value(val):
    return val == NULL_VALUE


if __name__ == '__main__':
    import doctest
    doctest.testmod()

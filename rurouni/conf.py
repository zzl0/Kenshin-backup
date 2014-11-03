# coding: utf-8
import os
from collections import OrderedDict
from ConfigParser import ConfigParser
from twisted.python import usage

from rurouni.exceptions import RurouniException


class OrderedConfigParser(ConfigParser):
    """
    Ordered Config Parser.

    http://stackoverflow.com/questions/1134071/keep-configparser-output-files-sorted.

    Acturally, from python 2.7 the ConfigParser default dict is `OrderedDict`,
    So we just rewrite the read method to check config file.
    """
    def read(self, path):
        if not os.access(path, os.R_OK):
            raise RurouniException(
                "Missing config file or wrong perm on %s" % path)
        return ConfigParser.read(self, path)


class Options(usage.Options):

    optParameters = [
        ['port', 'p', 2003, 'the port number to listen on.'],
        ['iface', None, 'localhost', 'the interface to listen on.'],
    ]


class Settings(object):
    CACHE_QUERY_PORT = '7002'
    CACHE_QUERY_INTERFACE = '0.0.0.0'

    LINE_RECEIVER_PORT = '2003'
    LINE_RECEIVER_INTERFACE = '0.0.0.0'

    MAX_CACHE_SIZE = 12

    CONF_DIR = '/Users/zzl/projects/bigdata/Kenshin/conf/'
    LOCAL_DATA_DIR = '/Users/zzl/projects/bigdata/Kenshin/data/'


settings = Settings()


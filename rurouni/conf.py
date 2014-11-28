# coding: utf-8
import os
from os.path import join, normpath, expanduser, dirname
from collections import OrderedDict
from ConfigParser import ConfigParser
from twisted.python import usage

from rurouni.exceptions import RurouniException
from rurouni import log


defaults = dict(
    CACHE_QUERY_PORT = '7002',
    CACHE_QUERY_INTERFACE = '0.0.0.0',

    LINE_RECEIVER_PORT = '2003',
    LINE_RECEIVER_INTERFACE = '0.0.0.0',

    PICKLE_RECEIVER_PORT = '2004',
    PICKLE_RECEIVER_INTERFACE = '0.0.0.0',

    DEFAULT_WAIT_TIME = 10,
    RUROUNI_METRIC_INTERVAL = 60,
    RUROUNI_METRIC = 'rurouni',

    LOG_UPDATES = True,
    CONF_DIR = '',
    LOCAL_DATA_DIR = '',
)

class Settings(dict):
    __getattr__ = dict.__getitem__

    def __init__(self):
        dict.__init__(self)
        self.update(defaults)

    def readFrom(self, path, section):
        parser = ConfigParser()
        if not parser.read(path):
            raise RurouniException("Failed to read config: %s" % path)

        if not parser.has_section(section):
            return

        for key, val in parser.items(section):
            key = key.upper()
            val_typ = type(defaults[key]) if key in defaults else str

            if val_typ is list:
                val = [v.strip() for v in value.split(',')]
            elif val_typ is bool:
                val = parser.getboolean(section, key)
            else:
                # attempt to figure out numeric types automatically
                try:
                    val = int(val)
                except:
                    try:
                        value = float(value)
                    except:
                        pass
            self[key] = val


settings = Settings()


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


class RurouniOptions(usage.Options):

    optFlags = [
        ["debug", "", "run in debug mode."],
    ]

    optParameters = [
        ['config', 'c', None, 'use the given config file.'],
        ['instance', '', 'a', 'manage a specific rurouni instance.']
    ]

    def postOptions(self):
        global settings
        section = 'cache'
        if self['config'] is not None:
            config = self['config']
            settings['CONF_DIR'] = dirname(self._normpath(config))
            settings.readFrom(config, section)
            settings.readFrom(config, "%s:%s" % (section, self['instance'] + '.metrics'))

        settings['instance'] = self['instance']
        settings['METRICS_FILE'] =  join(settings['LOCAL_DATA_DIR'], settings['instance'])

        if self['debug']:
            log.setDebugEnabled(True)

    @staticmethod
    def _normpath(path):
        return normpath(expanduser(path))

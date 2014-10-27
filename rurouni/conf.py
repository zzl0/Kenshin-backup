# coding: utf-8
from twisted.python import usage


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


settings = Settings()

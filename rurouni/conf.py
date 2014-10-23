# coding: utf-8
from twisted.python import usage


class Options(usage.Options):

    optParameters = [
        ['port', 'p', 2003, 'the port number to listen on.'],
        ['iface', None, 'localhost', 'the interface to listen on.'],
    ]

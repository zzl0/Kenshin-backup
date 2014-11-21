# coding: utf-8
from zope.interface import implements

from twisted.application.service import  IServiceMaker
from twisted.plugin import IPlugin

from rurouni import service
from rurouni import conf


class RurouniServiceMaker(object):
    implements(IServiceMaker, IPlugin)

    tapname = 'metric'
    description = 'a basic metric service.'
    options = conf.RurouniOptions

    def makeService(self, options):
        return service.createCacheService(options)


serviceMaker = RurouniServiceMaker()
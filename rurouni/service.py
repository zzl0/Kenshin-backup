# coding: utf-8
from twisted.application import internet, service
from twisted.application.internet import TCPServer
from twisted.plugin import IPlugin
from twisted.internet.protocol import ServerFactory

from rurouni import protocols
from rurouni import state, log
from rurouni.conf import settings


### root serveice

class RurouniRootService(service.MultiService):
    """ Root Service that properly configure twistd logging.
    """

    def setServiceParent(self, parent):
        service.MultiService.setServiceParent(self, parent)

        # TODO: configure logging.


def createBaseService(options):
    root_service = RurouniRootService()
    root_service.setName('rurouni')

    factory = ServerFactory()
    factory.protocol = protocols.MetricLineReceiver
    service = TCPServer(int(settings.LINE_RECEIVER_PORT), factory,
                        interface=settings.LINE_RECEIVER_INTERFACE)
    service.setServiceParent(root_service)

    return root_service


def createCacheService(options):
    from rurouni.cache import MetricCache
    from rurouni.protocols import CacheManagementHandler

    state.events.metricReceived.addHandler(MetricCache.store)
    root_service = createBaseService(options)

    factory = ServerFactory()
    factory.protocol = CacheManagementHandler
    service = TCPServer(int(settings.CACHE_QUERY_PORT), factory,
                        interface=settings.CACHE_QUERY_INTERFACE)
    service.setServiceParent(root_service)

    from rurouni.writer import WriterService
    service = WriterService()
    service.setServiceParent(root_service)

    return root_service

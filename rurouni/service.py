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

    receive_services = (
        (settings.LINE_RECEIVER_INTERFACE,
         settings.LINE_RECEIVER_PORT,
         protocols.MetricLineReceiver
        ),
        (settings.PICKLE_RECEIVER_INTERFACE,
         settings.PICKLE_RECEIVER_PORT,
         protocols.MetricPickleReceiver
        ),
        (settings.WHISPER_PICKLE_RECEIVER_INTERFACE,
         settings.WHISPER_PICKLE_RECEIVER_PORT,
         protocols.WhisperPickleReceiver
        )
    )
    for interface, port, protocol in receive_services:
        if port:
            factory = ServerFactory()
            factory.protocol = protocol
            service = TCPServer(int(port), factory, interface=interface)
            service.setServiceParent(root_service)

    from rurouni.state.instrumentation import InstrumentationService
    service = InstrumentationService()
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

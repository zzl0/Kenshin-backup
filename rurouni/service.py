# coding: utf-8
from twisted.python import log
from twisted.application import internet, service
from twisted.plugin import IPlugin
from twisted.internet.protocol import ServerFactory

from rurouni import protocols


### root serverice

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

    service = internet.TCPServer(int(options['port']), factory,
                                interface=options['iface'])
    service.setServiceParent(root_service)

    return root_service

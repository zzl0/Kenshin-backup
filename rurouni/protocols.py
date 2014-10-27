# coding: utf-8
from twisted.python import log
from twisted.internet.protocol import Protocol, ServerFactory
from twisted.protocols.basic import LineOnlyReceiver, Int32StringReceiver
from twisted.internet.error import ConnectionDone

from rurouni.state import events


### metric receiver

class MetricReceiver:
    """ Base class for all metric receive protocols.
    """
    def connectionMade(self):
        self.peerName = self.getPeerName()

    def getPeerName(self):
        if hasattr(self.transport, 'getPeer'):
            peer = self.transport.getPeer()
            return '%s:%d' % (peer.host, peer.port)
        else:
            return 'peer'

    def metricReceived(self, metric, datapoint):
        events.metricReceived(metric, datapoint)


class MetricLineReceiver(MetricReceiver, LineOnlyReceiver):
    delimiter = '\n'

    def lineReceived(self, line):
        try:
            metric, value, timestamp = line.strip().split()
            datapoint = (float(timestamp), float(value))
        except:
            log.msg('invalid line (%s) received from client %s' %
                    (line, self.peerName))
            return
        self.metricReceived(metric, datapoint)


class CacheReceiver(Int32StringReceiver):

    def connectionMade(self):
        peer = self.transport.getPeer()
        self.peerAddr = "%s:%s" % (peer.host, peer.port)
        log.msg("%s connected" % self.peerAddr)

    def connectionLost(self, reason):
        if reason.check(ConnectionDone):
            log.msg("%s disconnected" % self.peerAddr)
        else:
            log.msg("%s connection lost: %s" % (self.peerAddr, reason.value))

    def stringReceived(self, rawRequest):
        log.msg(" %s" % rawRequest)

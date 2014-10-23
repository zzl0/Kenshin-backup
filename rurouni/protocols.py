# coding: utf-8
from twisted.python import log
from twisted.internet.protocol import Protocol, ServerFactory
from twisted.protocols.basic import LineOnlyReceiver


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
        log.msg('metric %s, val: %s' % (metric, datapoint))


class MetricLineReceiver(MetricReceiver, LineOnlyReceiver):
    delimiter = '\n'

    def lineReceived(self, line):
        try:
            metric, value, timestamp = line.strip().split()
            datapoint = (float(timestamp), float(value))
            self.metricReceived(metric, datapoint)
        except:
            log.msg('invalid line (%s) received from client %s' %
                    (line, self.peerName))


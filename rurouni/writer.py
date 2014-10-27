# coding: utf-8
import time

from twisted.application.service import Service
from twisted.python import log
from twisted.internet import reactor

from rurouni.cache import MetricCache


class WriterService(Service):

    def __init__(self):
        pass

    def startService(self):
        reactor.callInThread(writeForever)
        Service.startService(self)

    def stopService(self):
        Service.stopService(self)


def writeForever():
    while reactor.running:
        try:
            writeCachedDataPoints()
        except:
            log.err()
        # The writer thread only sleeps when cache is empty
        # or an error occurs
        time.sleep(1)


def writeCachedDataPoints():
    """
    Write datapoints until the MetricCache is completely empty.
    """
    metrics = MetricCache.counts()

    while MetricCache:
        for metric, queueSize in metrics:
            datapoints = MetricCache.pop(metric)
            log.msg('write metric: %s, datapoints: %s' % (metric, datapoints))
            time.sleep(10)

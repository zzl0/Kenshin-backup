# coding: utf-8
import time

from twisted.application.service import Service
from twisted.internet import reactor

from rurouni.cache import MetricCache
from rurouni import log


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
        except Exception as e:
            log.err('write error: %s' % e)
            raise e
        # The writer thread only sleeps when cache is empty
        # or an error occurs
        time.sleep(1)


def writeCachedDataPoints():
    metrics = MetricCache.counts()
    log.msg("write metrics: %s" % metrics)

    for metric, idx in metrics:
        datapoints = MetricCache.pop(metric, idx)
        log.msg('write metric: %s, datapoints: %s' % (metric, datapoints))
        time.sleep(1)

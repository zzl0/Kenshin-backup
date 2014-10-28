# coding: utf-8

from threading import Lock
from twisted.python import log

from rurouni.conf import settings
from rurouni import state


class MetricCache(dict):

    def __init__(self):
        self.size = 0
        self.lock = Lock()

    def __setitem__(self, key, value):
        raise TypeError("Use store() method instead!")

    def store(self, metric, datapoint):
        log.msg("MetricCache received %s: %s" % (metric, datapoint))
        try:
            self.lock.acquire()
            self.setdefault(metric, []).append(datapoint)
            self.size += 1
        finally:
            self.lock.release()

        if self.isFull():
            log.msg("MetricCache is full: self.size=%d" % self.size)
            state.events.cacheFull()

    def isFull(self):
        return self.size >= settings.MAX_CACHE_SIZE

    def pop(self, metric):
        try:
            self.lock.acquire()
            datapoints = dict.pop(self, metric)
            self.size -= len(datapoints)
            return datapoints
        finally:
            self.lock.release()

    def counts(self):
        try:
            self.lock.acquire()
            return [(metric, len(datapoints)) for (metric, datapoints) in self.items()]
        finally:
            self.lock.release()


# Ghetto singleton
MetricCache = MetricCache()

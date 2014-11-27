# coding: utf-8
import os
import time

from twisted.application.service import Service
from twisted.internet import reactor

import kenshin
from rurouni.cache import MetricCache
from rurouni import log
from rurouni.conf import settings
from rurouni.state import instrumentation
from rurouni.storage import getFilePath, getSchema


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
            write = writeCachedDataPoints()
        except Exception as e:
            log.err('write error: %s' % e)
            raise e
        # The writer thread only sleeps when there is no write
        # or an error occurs
        if not write:
            time.sleep(1)


def writeCachedDataPoints():
    metrics = MetricCache.writableFileCaches()
    log.debug("metrics=============: %s" % metrics)
    if not metrics:
        return False

    for metric, idx in metrics:
        datapoints = MetricCache.pop(metric, idx)
        file_path = getFilePath(metric, idx)

        try:
            t1 = time.time()
            log.debug('filepath: %s, datapoints: %s' % (file_path, datapoints))
            kenshin.update(file_path, datapoints)
            t2 = time.time()
            update_time = t2 - t1
        except Exception as e:
            log.err('Error writing to %s: %s' % (file_path, e))
            instrumentation.incr('errors')
        else:
            point_cnt = len(datapoints)
            instrumentation.incr('committedPoints', point_cnt)
            instrumentation.append('updateTimes', update_time)

            if settings.LOG_UPDATES:
                log.updates("wrote %d datapoints for %s in %.5f secs" %
                            (point_cnt, metric, update_time))

    return True

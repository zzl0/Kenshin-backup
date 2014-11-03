# coding: utf-8
import os
import time

from twisted.application.service import Service
from twisted.internet import reactor

import kenshin
from rurouni.cache import MetricCache
from rurouni import log
from rurouni.storage import getFilePath, loadStorageSchemas


schemas = loadStorageSchemas()


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
    metrics = MetricCache.counts()
    if not metrics:
        return False

    for metric, idx in metrics:
        tags, datapoints = MetricCache.pop(metric, idx)
        log.msg('write metric: %s, datapoints: %s' % (metric, datapoints))
        file_path = getFilePath(metric, idx)

        if not os.path.exists(file_path):
            for schema in schemas:
                if schema.match(metric):
                    log.creates('new metric file %s-%d matched schema %s' %
                                (metric, idx, schema.name))
                    break
            kenshin.create(file_path, tags, schema.archives, schema.xFilesFactor,
                           schema.aggregationMethod)
        log.msg('file path: %s, data: %s' % (file_path, datapoints))
        kenshin.update(file_path, datapoints)

    return True

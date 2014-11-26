# coding: utf-8
import os
import time
from threading import Lock

import kenshin
from rurouni import log
from rurouni.conf import settings
from rurouni.storage import getFilePath, getSchema, createLink


class MetricCache(object):
    """
    (schema, file_idx, pos_idx)
    """
    def __init__(self):
        self.lock = Lock()
        self.metric_idxs = {}
        self.schema_caches = {}
        self.metrics_fh = None

    def __del__(self):
        self.metrics_fh.close()

    def initCache(self):
        try:
            self.lock.acquire()
            metrics_file = settings.METRICS_FILE
            if os.path.exists(metrics_file):
                with open(metrics_file) as f:
                    for line in f:
                        metric = line.rstrip('\n')
                        schema = getSchema(metric)
                        schema_cache = self.getSchemaCache(schema)
                        file_idx = schema_cache.getFileCacheIdx(schema, init=True)
                        pos_idx = schema_cache[file_idx].getPosIdx(schema)
                        metric_idx = (schema.name, file_idx, pos_idx)
                        self.metric_idxs[metric] = metric_idx
            log.debug("%s" % self.metric_idxs)
            self.metrics_fh = open(metrics_file, 'a')
        finally:
            self.lock.release()

    def put(self, metric, datapoint):
        log.debug("MetricCache received (%s, %s)" % (metric, datapoint))
        (schema_name, file_idx, pos_idx) = self.getMetricIdx(metric)
        file_cache = self.schema_caches[schema_name][file_idx]
        file_cache.add(pos_idx, datapoint)

    def getMetricIdx(self, metric):
        try:
            self.lock.acquire()
            if metric in self.metric_idxs:
                return self.metric_idxs[metric]
            else:
                schema = getSchema(metric)
                schema_cache = self.getSchemaCache(schema)
                file_idx = schema_cache.getFileCacheIdx(schema)
                pos_idx = schema_cache[file_idx].getPosIdx(schema)

                # create link
                file_path = getFilePath(schema.name, file_idx)
                self.metrics_fh.write(metric + '\n')
                createLink(metric, file_path)

                metric_idx = (schema.name, file_idx, pos_idx)
                self.metric_idxs[metric] = metric_idx
                return metric_idx
        finally:
            self.lock.release()

    def getSchemaCache(self, schema):
        try:
            return self.schema_caches[schema.name]
        except:
            schema_cache = SchemaCache()
            self.schema_caches[schema.name] = schema_cache
            return schema_cache

    def get(self, metric):
        if metric not in self.metrics:
            return []
        (schema_name, file_idx, pos_idx) = self.getMetricIdx(metric)
        file_cache = self.schema_caches[schema_name][file_idx]
        now = int(time.time())
        data = file_cache.get(end_ts=now)
        return [(ts, val[pos_idx]) for ts, val in data]

    def gets(self, metric):
        pass

    def pop(self, schema_name, file_idx):
        file_cache = self.schema_caches[schema_name][file_idx]
        datapoints = file_cache.get(clear=True)
        log.debug('canWrite: %s' % file_cache.canWrite())
        return datapoints

    def writableFileCaches(self):
        try:
            self.lock.acquire()
            return[(schema_name, file_idx)
                   for (schema_name, schema_cache) in self.schema_caches.items()
                   for file_idx in range(schema_cache.size())
                   if schema_cache[file_idx].canWrite()]
        finally:
            self.lock.release()


class SchemaCache(object):
    def __init__(self):
        self.file_caches = []
        self.curr_idx = -1

    def __getitem__(self, idx):
        return self.file_caches[idx]

    def size(self):
        return self.curr_idx + 1

    def getFileCacheIdx(self, schema, init=False):
        if (self.curr_idx == -1 or
                self.file_caches[self.curr_idx].metricFull()):
            file_cache = FileCache(schema)
            self.curr_idx += 1
            self.file_caches.append(file_cache)

            if not init:
                # create file
                file_path = getFilePath(schema.name, self.curr_idx)
                tags = ['N'] * schema.metrics_num
                kenshin.create(file_path, tags, schema.archives, schema.xFilesFactor,
                               schema.aggregationMethod)
        return self.curr_idx


class FileCache(object):
    def __init__(self, schema):
        self.lock = Lock()
        self.curr_size = 0
        self.metrics_num = schema.metrics_num
        self.resolution = schema.archives[0][0]
        self.retention = schema.cache_retention

        self.points_num = self.retention / self.resolution
        self.cache_size = int(self.points_num * schema.cache_ratio)
        self.points = [0] * self.metrics_num * self.cache_size
        self.base_idxs = [0] * self.metrics_num

        self.start_ts = None
        self.start_offset = 0
        self.can_write = False

    def getPosIdx(self, schema):
        try:
            self.lock.acquire()
            curr_size = self.curr_size
            self.curr_size += 1
            return curr_size
        finally:
            self.lock.release()

    def metricFull(self):
        try:
            self.lock.acquire()
            return self.curr_size == self.metrics_num
        finally:
            self.lock.release()

    def metricEmpty(self):
        try:
            self.lock.acquire()
            return not self.curr_size
        finally:
            self.lock.release()

    def canWrite(self):
        try:
            self.lock.acquire()
            return self.can_write
        finally:
            self.lock.release()

    def add(self, pos_idx, datapoint):
        try:
            self.lock.acquire()
            if pos_idx not in self.base_idxs:
                self.base_idxs[pos_idx] = self.cache_size * pos_idx

            base_idx = self.base_idxs[pos_idx]
            ts, val = datapoint

            if self.start_ts is None:
                self.start_ts = ts - ts % self.resolution
                self.start_offset = 0
                offset = 0
            else:
                offset = (ts - self.start_ts) / self.resolution
            idx = base_idx + (self.start_offset + offset) % self.cache_size

            # can write
            if ts - self.start_ts - self.retention >= settings.DEFAULT_WAIT_TIME:
                self.can_write = True

            log.debug("add idx: %s, ts %s, start_ts: %s, start_offset: %s, retention: %s" %
                      (idx, ts, self.start_ts, self.start_offset, self.retention))
            self.points[idx] = val
        except Exception as e:
            log.err('add error in FileCache: %s' % e)
        finally:
            self.lock.release()

    def get_offset(self, ts):
        offset = self.start_offset + (ts - self.start_ts) / self.resolution
        return offset % self.cache_size

    def get(self, end_ts=None, clear=False):
        if self.metricEmpty():
            return []
        try:
            self.lock.acquire()
            begin_offset = self.start_offset
            if end_ts:
                end_offset = self.get_offset(end_ts)
            else:
                end_offset = (begin_offset + self.points_num) % self.cache_size

            log.debug("begin_offset: %s, end_offset: %s" %
                      (begin_offset, end_offset))

            rs = [None] * self.metrics_num
            if begin_offset < end_offset:
                length = end_offset - begin_offset
                for i, base_idx in enumerate(self.base_idxs[:self.curr_size]):
                    begin_idx = base_idx + begin_offset
                    end_idx = base_idx + end_offset
                    val = self.points[begin_idx: end_idx]
                    rs[i] = val
                    if clear:
                        self.clearPoint(begin_idx, end_idx)
            else:
                # wrap around
                length = self.cache_size - begin_offset + end_offset
                for i, base_idx in enumerate(self.base_idxs[:self.curr_size]):
                    begin_idx = base_idx + begin_offset
                    end_idx = base_idx + end_offset
                    val = self.points[begin_idx: base_idx+self.cache_size]
                    val += self.points[base_idx: begin_idx]
                    rs[i] = val
                    if clear:
                        self.clearPoint(begin_idx, base_idx+self.cache_size)
                        self.clearPoint(base_idx, end_idx)

            # empty metrics
            for j in range(i+1, self.metrics_num):
                rs[j] = [0] * length

            # timestamps
            timestamps = [self.start_ts + i * self.resolution
                          for i in range(length)]

            if clear:
                self.start_offset = end_offset
                self.start_ts = timestamps[-1] + self.resolution
                self.can_write = False

            return zip(timestamps, zip(*rs))

        finally:
            self.lock.release()

    def clearPoint(self, begin_idx, end_idx):
        for i in range(begin_idx, end_idx):
            self.points[i] = 0


MetricCache = MetricCache()

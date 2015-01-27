# coding: utf-8
import os
import time
from threading import Lock

import kenshin
from kenshin.consts import NULL_VALUE
from kenshin.utils import is_null_value
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
        if self.metrics_fh is not None:
            self.metrics_fh.close()

    def initCache(self):
        with self.lock:
            # avoid repeated call
            if self.metrics_fh is not None:
                return

            metrics_file = settings.METRICS_FILE
            if os.path.exists(metrics_file):
                with open(metrics_file) as f:
                    for line in f:
                        line = line.strip('\n')
                        metric, file_idx, file_pos = line.rsplit(" ", 2)
                        schema = getSchema(metric)
                        schema_cache = self.getSchemaCache(schema)
                        schema_cache.add(schema, int(file_idx), int(file_pos))
                        self.metric_idxs[metric] = (schema.name, int(file_idx), int(file_pos))

            self.metrics_fh = open(metrics_file, 'a')

    def put(self, metric, datapoint):
        log.debug("MetricCache received (%s, %s)" % (metric, datapoint))
        (schema_name, file_idx, pos_idx) = self.getMetricIdx(metric)
        file_cache = self.schema_caches[schema_name][file_idx]
        file_cache.put(pos_idx, datapoint)

    def getMetricIdx(self, metric):
        with self.lock:
            if metric in self.metric_idxs:
                return self.metric_idxs[metric]
            else:
                schema = getSchema(metric)
                schema_cache = self.getSchemaCache(schema)
                file_idx = schema_cache.getFileCacheIdx(schema)
                pos_idx = schema_cache[file_idx].getPosIdx()

                # create link
                file_path = getFilePath(schema.name, file_idx)
                self.metrics_fh.write("%s %s %s" % (metric, file_idx, pos_idx) + '\n')
                kenshin.add_tag(metric, file_path, pos_idx)
                createLink(metric, file_path)
                self.metric_idxs[metric] = (schema.name, file_idx, pos_idx)
                return self.metric_idxs[metric]

    def getSchemaCache(self, schema):
        try:
            return self.schema_caches[schema.name]
        except:
            schema_cache = SchemaCache()
            self.schema_caches[schema.name] = schema_cache
            return schema_cache

    def get(self, metric):
        if metric not in self.metric_idxs:
            return []
        (schema_name, file_idx, pos_idx) = self.getMetricIdx(metric)
        file_cache = self.schema_caches[schema_name][file_idx]
        now = int(time.time())
        data = file_cache.get(end_ts=now)
        return [(ts, val[pos_idx]) for ts, val in data
                                   if not is_null_value(val[pos_idx])]

    def pop(self, schema_name, file_idx):
        file_cache = self.schema_caches[schema_name][file_idx]
        datapoints = file_cache.get(clear=True)
        log.debug('canWrite: %s' % file_cache.canWrite())
        return datapoints

    def writableFileCaches(self):
        with self.lock:
            return[(schema_name, file_idx)
                   for (schema_name, schema_cache) in self.schema_caches.items()
                   for file_idx in range(schema_cache.size())
                   if schema_cache[file_idx].canWrite()]


class SchemaCache(object):
    def __init__(self):
        self.file_caches = []
        self.curr_idx = 0

    def __getitem__(self, idx):
        return self.file_caches[idx]

    def size(self):
        return len(self.file_caches)

    def getFileCacheIdx(self, schema):
        while self.curr_idx < len(self.file_caches):
            if not self.file_caches[self.curr_idx].metricFull():
                return self.curr_idx
            else:
                self.curr_idx += 1
        # there is no file cache avaiable, we create a new one
        cache = FileCache(schema)
        self.file_caches.append(cache)

        # create file
        file_path = getFilePath(schema.name, self.curr_idx)
        tags = [''] * schema.metrics_max_num
        kenshin.create(file_path, tags, schema.archives, schema.xFilesFactor,
                       schema.aggregationMethod)
        return self.curr_idx

    def add(self, schema, file_idx, file_pos):
        if len(self.file_caches) <= file_idx:
            for i in range(len(self.file_caches), file_idx + 1):
                self.file_caches.append(FileCache(schema))
        self.file_caches[file_idx].add(file_pos)


class FileCache(object):
    def __init__(self, schema):
        self.lock = Lock()
        self.metrics_max_num = schema.metrics_max_num
        self.bitmap = 0
        self.avaiable_pos_idx = 0
        self.resolution = schema.archives[0][0]
        self.retention = schema.cache_retention

        # +1 to avoid self.points_num == 0
        self.points_num = self.retention / self.resolution + 1
        self.cache_size = int(self.points_num * schema.cache_ratio)
        self.points = [NULL_VALUE] * self.metrics_max_num * self.cache_size
        self.base_idxs = [i * self.cache_size for i in xrange(self.metrics_max_num)]

        self.start_ts = None
        self.start_offset = 0
        self.can_write = False

    def add(self, file_pos):
        with self.lock:
            self.bitmap |= (1 << file_pos)

    def getPosIdx(self):
        with self.lock:
            while True:
                if self.bitmap & (1 << self.avaiable_pos_idx):
                    self.avaiable_pos_idx += 1
                else:
                    self.bitmap |= (1 << self.avaiable_pos_idx)
                    self.avaiable_pos_idx += 1
                    return self.avaiable_pos_idx - 1

    def metricFull(self):
        with self.lock:
            return self.bitmap + 1 == (1 << self.metrics_max_num)

    def metricEmpty(self):
        with self.lock:
            return not self.start_ts

    def canWrite(self):
        with self.lock:
            return self.can_write

    def setWrite(self, timestamp):
        """
        Check write condition, if this cache can write to file,
        then set canWrite flag.
        """
        if not self.can_write and \
           (timestamp - self.start_ts - self.retention >= settings.DEFAULT_WAIT_TIME):
            self.can_write = True

    def put(self, pos_idx, datapoint):
        log.debug("retention: %s, cache_size: %s, points_num: %s" %
                  (self.retention, self.cache_size, self.points_num))
        with self.lock:
            try:
                base_idx = self.base_idxs[pos_idx]
                ts, val = datapoint

                if self.start_ts is None:
                    self.start_ts = ts - ts % self.resolution
                    self.start_offset = 0
                    offset = 0
                else:
                    offset = (ts - self.start_ts) / self.resolution
                idx = base_idx + (self.start_offset + offset) % self.cache_size

                log.debug("put idx: %s, ts: %s, start_ts: %s, start_offset: %s, retention: %s" %
                          (idx, ts, self.start_ts, self.start_offset, self.retention))
                self.setWrite(ts)
                self.points[idx] = val
            except Exception as e:
                log.err('put error in FileCache: %s' % e)

    def get_offset(self, ts):
        offset = self.start_offset + (ts - self.start_ts) / self.resolution
        return offset % self.cache_size

    def get(self, end_ts=None, clear=False):
        if self.metricEmpty():
            return []
        with self.lock:
            begin_offset = self.start_offset
            if end_ts:
                end_offset = self.get_offset(end_ts)
            else:
                end_offset = (begin_offset + self.points_num) % self.cache_size

            log.debug("begin_offset: %s, end_offset: %s" %
                      (begin_offset, end_offset))

            rs = [None] * self.metrics_max_num
            if begin_offset < end_offset:
                length = end_offset - begin_offset
                for i, base_idx in enumerate(self.base_idxs):
                    begin_idx = base_idx + begin_offset
                    end_idx = base_idx + end_offset
                    val = self.points[begin_idx: end_idx]
                    rs[i] = val
                    if clear:
                        self.clearPoint(begin_idx, end_idx)
            else:
                # wrap around
                length = self.cache_size - begin_offset + end_offset
                for i, base_idx in enumerate(self.base_idxs):
                    begin_idx = base_idx + begin_offset
                    end_idx = base_idx + end_offset
                    val = self.points[begin_idx: base_idx+self.cache_size]
                    val += self.points[base_idx: begin_idx]
                    rs[i] = val
                    if clear:
                        self.clearPoint(begin_idx, base_idx+self.cache_size)
                        self.clearPoint(base_idx, end_idx)

            # timestamps
            timestamps = [self.start_ts + i * self.resolution
                          for i in range(length)]

            if clear:
                self.start_offset = end_offset
                self.start_ts = timestamps[-1] + self.resolution
                self.can_write = False

            return zip(timestamps, zip(*rs))

    def clearPoint(self, begin_idx, end_idx):
        for i in range(begin_idx, end_idx):
            self.points[i] = NULL_VALUE


MetricCache = MetricCache()

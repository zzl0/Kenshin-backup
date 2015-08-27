# coding: utf-8
import os
import time
from threading import Lock

import kenshin
from kenshin.consts import NULL_VALUE
from rurouni import log
from rurouni.conf import settings
from rurouni.storage import getFilePath, createLink, StorageSchemas


class MetricCache(object):
    """
    (schema, file_idx, pos_idx)
    """
    def __init__(self):
        self.lock = Lock()
        self.metric_idxs = {}
        self.schema_caches = {}
        self.metrics_fh = None
        self.storage_schemas = None

    def __del__(self):
        if self.metrics_fh is not None:
            self.metrics_fh.close()

    def initStorageSchemas(self):
        if self.storage_schemas is None:
            conf_file = os.path.join(settings.CONF_DIR, 'storage-schemas.conf')
            self.storage_schemas = StorageSchemas(conf_file)

    def initCache(self):
        with self.lock:
            # avoid repeated call
            if self.metrics_fh is not None:
                return

            self.initStorageSchemas()
            metrics_file = settings.METRICS_FILE
            if os.path.exists(metrics_file):
                MAX_ALLOW_ERR_LINE = 1
                err_line_cnt = 0
                with open(metrics_file) as f:
                    for line in f:
                        line = line.strip('\n')
                        try:
                            metric, schema_name, file_idx, file_pos = line.split(" ")
                            file_idx = int(file_idx)
                            file_pos = int(file_pos)
                        except Exception as e:
                            if err_line_cnt < MAX_ALLOW_ERR_LINE:
                                err_line_cnt += 1
                                continue
                            else:
                                raise Exception('Index file has many error: %s' % e)

                        schema = self.storage_schemas.getSchemaByName(schema_name)
                        schema_cache = self.getSchemaCache(schema)
                        schema_cache.add(schema, file_idx, file_pos)
                        self.metric_idxs[metric] = (schema.name, file_idx, file_pos)

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
                schema = self.storage_schemas.getSchemaByMetric(metric)
                schema_cache = self.getSchemaCache(schema)
                file_idx = schema_cache.getFileCacheIdx(schema)
                pos_idx = schema_cache[file_idx].getPosIdx()

                # create file
                file_path = getFilePath(schema.name, file_idx)
                if not os.path.exists(file_path):
                    tags = [''] * schema.metrics_max_num
                    kenshin.create(file_path, tags, schema.archives, schema.xFilesFactor,
                                   schema.aggregationMethod)
                # update file metadata
                kenshin.add_tag(metric, file_path, pos_idx)
                # create link
                createLink(metric, file_path)
                # create index
                self.metrics_fh.write("%s %s %s %s\n" % (metric, schema.name, file_idx, pos_idx))

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
                                   if val[pos_idx] != NULL_VALUE]

    def pop(self, schema_name, file_idx, end_ts=None, clear=True):
        file_cache = self.schema_caches[schema_name][file_idx]
        datapoints = file_cache.get(end_ts=end_ts, clear=clear)
        return datapoints

    def writableFileCaches(self):
        now = int(time.time())
        with self.lock:
            return[(schema_name, file_idx)
                   for (schema_name, schema_cache) in self.schema_caches.items()
                   for file_idx in range(schema_cache.size())
                   if schema_cache[file_idx].canWrite(now)]

    def getAllFileCaches(self):
        return [(schema_name, file_idx)
                for (schema_name, schema_cache) in self.schema_caches.iteritems()
                for file_idx in range(schema_cache.size())]


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
        return self.curr_idx

    def add(self, schema, file_idx, file_pos):
        if len(self.file_caches) <= file_idx:
            for _ in range(len(self.file_caches), file_idx + 1):
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
        self.max_ts = 0
        self.start_offset = 0

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

    def canWrite(self, now):
        with self.lock:
            return self.start_ts and ((now - self.start_ts - self.retention) >=
                                      settings.DEFAULT_WAIT_TIME)

    def put(self, pos_idx, datapoint):
        log.debug("retention: %s, cache_size: %s, points_num: %s" %
                  (self.retention, self.cache_size, self.points_num))
        with self.lock:
            try:
                base_idx = self.base_idxs[pos_idx]
                ts, val = datapoint

                self.max_ts = max(self.max_ts, ts)
                if self.start_ts is None:
                    self.start_ts = ts - ts % self.resolution
                    idx = base_idx
                else:
                    offset = (ts - self.start_ts) / self.resolution
                    idx = base_idx + (self.start_offset + offset) % self.cache_size

                log.debug("put idx: %s, ts: %s, start_ts: %s, start_offset: %s, retention: %s" %
                          (idx, ts, self.start_ts, self.start_offset, self.retention))
                self.points[idx] = val
            except Exception as e:
                log.err('put error in FileCache: %s' % e)

    def get_offset(self, ts):
        interval = (ts - self.start_ts) / self.resolution
        if interval >= self.cache_size:
            interval = self.cache_size - 1
        return (self.start_offset + interval) % self.cache_size

    def get(self, end_ts=None, clear=False):
        if self.metricEmpty():
            return []
        with self.lock:
            begin_offset = self.start_offset
            if end_ts:
                end_offset = self.get_offset(end_ts)
            else:
                end_offset = (begin_offset + self.points_num) % self.cache_size

            log.debug("begin_offset: %s, end_offset: %s end_ts: %s, clear: %s" %
                      (begin_offset, end_offset, end_ts, clear,))

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
                next_ts = timestamps[-1] + self.resolution
                if self.max_ts < next_ts:
                    self.start_ts = None
                    self.start_offset = 0
                else:
                    self.start_ts = next_ts
                    self.start_offset = end_offset

            return zip(timestamps, zip(*rs))

    def clearPoint(self, begin_idx, end_idx):
        for i in range(begin_idx, end_idx):
            self.points[i] = NULL_VALUE


MetricCache = MetricCache()

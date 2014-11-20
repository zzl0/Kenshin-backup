# coding: utf-8
import os
import time
from threading import Lock
from collections import OrderedDict

from rurouni.conf import settings
from rurouni import state, log
from rurouni.storage import getSchema


class MetricData(object):
    """
    The data structure of metric data.

    The logic structure is as following:


                      1414661115
                          |
                          v
                        +---+---+---+---+---+---+
    host=server0,cpu=0->| 8 |   |   |   |   |   |
                        +---+---+---+---+---+---+
                        |   |   |   |   |   |   |
                        +---+---+---+---+---+---+
                        |   |   |   |   |   |   |
                        +---+---+---+---+---+---+
                        |   |   |   |   |   |   |
                        +---+---+---+---+---+---+

    - each row represent a specific tags.
    - each col represent values of different timestamps.

    Actually we use one-dimention list to implement above structure.
    """

    def __init__(self, schema):
        self.tags_num = schema.tags_num
        self.resolution = schema.archives[0][0]
        self.retention = schema.cache_retention
        self.lock = Lock()

        self.points_num = self.retention / self.resolution
        self.points_num_max = int(self.points_num * 1.2)
        self.points = [0] * self.tags_num * self.points_num_max
        self.tags_dict = OrderedDict()

        self.start_ts = None
        self.start_idx = 0
        self.can_write = False
        self.too_full = False

    def init_tags(self, tags_list):
        for i, tags in enumerate(tags_list):
            self.tags_dict[tags] = i * self.points_num_max

    def get_tags_list(self):
        return self.tags_dict.keys() + ['N'] * (self.tags_num - self.size())

    def full(self):
        return len(self.tags_dict) == self.tags_num

    def canWrite(self):
        try:
            self.lock.acquire()
            return self.can_write
        finally:
            self.lock.release()

    def clearWriteFlag(self):
        self.can_write = False

    def size(self):
        return len(self.tags_dict)

    def add(self, tags, datapoint):
        try:
            self.lock.acquire()
            if tags not in self.tags_dict:
                self.tags_dict[tags] = self.size() * self.points_num_max

            base_idx = self.tags_dict[tags]
            ts, val = datapoint

            if self.start_ts is None:
                self.start_ts = ts - ts % self.resolution
                self.start_idx = base_idx
                offset = 0
            else:
                offset = (ts - self.start_ts) / self.resolution
            idx = base_idx + (self.start_idx + offset) % self.points_num_max

            if ts - self.start_ts - self.retention >= settings.DEFAULT_WAIT_TIME:
                self.can_write = True

            log.debug("add idx: %s, start_ts: %s, start_idx: %s" % (
                       idx, self.start_ts, self.start_idx))
            self.points[idx] = val
        except Exception as e:
            log.err('add error in MetricData: %s' % e)
        finally:
            self.lock.release()

    def get_idx(self, timestamp):
        relative_idx = self.start_idx + (timestamp - self.start_ts) / self.resolution
        return relative_idx % self.points_num_max

    def align_timestamp(self, timestamp):
        return timestamp - (timestamp % self.resolution)

    def read(self, begin_ts=None, end_ts=None, clear=False):
        try:
            self.lock.acquire()
            tags_list = self.get_tags_list()

            if not begin_ts:
                if not self.start_ts:
                    return tags_list, []
                begin_ts = self.start_ts
            begin_idx = self.get_idx(begin_ts)
            if end_ts:
                end_idx = self.get_idx(end_ts)
            else:
                end_idx = (begin_idx+self.points_num) % self.points_num_max

            log.debug("begin_idx: %s, end_idx: %s, points_num: %s, points_num_max: %s" % (
                       begin_idx, end_idx, self.points_num, self.points_num_max))

            rs = [None] * self.tags_num
            if begin_idx < end_idx:
                length = end_idx - begin_idx
                for i, (_, base_idx) in enumerate(self.tags_dict.items()):
                    val = self.points[base_idx+begin_idx: base_idx+end_idx]
                    rs[i] = val
                    if clear:
                        self.clearPoint(base_idx+begin_idx, base_idx+end_idx)
            else:  # wrap around
                length = self.points_num_max - begin_idx + end_idx
                for i, (_, base_idx) in enumerate(self.tags_dict.items()):
                    val = self.points[base_idx+begin_idx: base_idx+self.points_num_max]
                    val += self.points[base_idx: base_idx+end_idx]
                    rs[i] = val
                    if clear:
                        self.clearPoint(base_idx+begin_idx, base_idx+self.points_num_max)
                        self.clearPoint(base_idx, base_idx+end_idx)

            # empty tags
            for j in range(i+1, self.tags_num):
                rs[j] = [0] * length

            # timestamps
            begin_ts = self.align_timestamp(begin_ts)
            timestamps = [begin_ts + i * self.resolution for i in range(length)]

            if clear:
                self.start_idx = end_idx
                self.start_ts = self.start_ts + self.resolution * length
                self.clearWriteFlag()

            return tags_list, zip(timestamps, zip(*rs))
        finally:
            self.lock.release()

    def clearPoint(self, begin_idx, end_idx):
        for i in range(begin_idx, end_idx):
            self.points[i] = 0


class MetricCache(dict):
    """
    The structure of cache as following:

    {
        metric_name: [
            MetricData,
        ]
    }
    """
    def __init__(self):
        self.cache = {}
        self.lock = Lock()
        self.metrics_fh = None

    def initCache(self):
        try:
            self.lock.acquire()
            if self.metrics_fh is not None:
                return

            metrics_file = settings.METRICS_FILE
            if os.path.exists(metrics_file):
                with open(metrics_file) as f:
                    rs = {}
                    for line in f:
                        metric, tags = line.rstrip('\n').split('\t')
                        rs.setdefault(metric, [])
                        rs[metric].append(tags)
                    for metric, tags_list in rs.items():
                        self.cache[metric] = []
                        schema = getSchema(metric)
                        tags_num = schema.tags_num
                        for i in range(0, len(tags_list), tags_num):
                            metric_data = MetricData(schema)
                            metric_data.init_tags(tags_list[i:tags_num])
                            self.cache[metric].append(metric_data)
            self.metrics_fh = open(metrics_file, 'a')
        finally:
            self.lock.release()

    def store(self, metric, tags, datapoint=None):
        log.debug("MetricCache received (%s, %s, %s)" % (metric, tags, datapoint))
        try:
            self.lock.acquire()
            if metric in self.cache:
                metric_data_list = self.cache[metric]
                flag = False
                for metric_data in metric_data_list:
                    if tags in metric_data.tags_dict:
                        flag = True
                        break

                if not flag:
                    self._record_new_metric(metric, tags)
                    if metric_data.full():
                        schema = getSchema(metric)
                        metric_data = MetricData(schema)
                        metric_data_list.append(metric_data)
            else:
                self._record_new_metric(metric, tags)
                schema = getSchema(metric)
                metric_data = MetricData(schema)
                self.cache[metric] = [metric_data]
            self.lock.release()
            metric_data.add(tags, datapoint)
        except Exception as e:
            self.lock.release()

    def _record_new_metric(self, metric, tags):
        self.metrics_fh.write('%s\t%s\n' % (metric, tags))

    def fetch(self, metric, tags):
        now = int(time.time())
        for metric_data in self.cache[metric]:
            if tags in metric_data.tags_dict:
                tags_list, datapoints = metric_data.read(end_ts=now)
                tags_idx = tags_list.index(tags)
                log.debug("tagslist: %s, idx: %s" % (tags_list, tags_idx))
                return [(x[0], x[1][tags_idx]) for x in datapoints]

    def pop(self, metric, metric_data_idx):
        metric_data = self.cache[metric][metric_data_idx]
        tags, datapoints = metric_data.read(clear=True)
        log.debug("canWrite: %s" % metric_data.canWrite())
        return tags, datapoints

    def counts(self):
        try:
            self.lock.acquire()
            return [(metric, i) for metric in self.cache
                            for i, metric_data in enumerate(self.cache[metric])
                            if metric_data.canWrite()]
        finally:
            self.lock.release()


# Ghetto singleton
MetricCache = MetricCache()

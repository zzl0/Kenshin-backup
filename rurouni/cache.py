# coding: utf-8

from threading import Lock
from collections import OrderedDict

from rurouni.conf import settings
from rurouni import state, log


DEFAULT_TAGS_NUM = 40
DEFAULT_RESOLUTION = 1    # 1 points per second
DEFAULT_RETENTION = 10   # 600 seconds


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

    Acturally we use one-dimention list to implement above structure.
    """

    def __init__(self, tags_num=None, resolution=None, retention=None):
        self.tags_num = tags_num or DEFAULT_TAGS_NUM
        self.resolution = resolution or DEFAULT_RESOLUTION
        self.retention = retention or DEFAULT_RETENTION

        self.points_num = self.retention / self.resolution
        self.points_num_max = int(self.points_num * 1.2)
        self.points = [0] * self.tags_num * self.points_num_max
        self.tags_dict = OrderedDict()

        self.start_ts = None
        self.start_idx = 0
        self.can_write = False
        self.too_full = False

    def full(self):
        return len(self.tags_dict) == self.tags_num

    def canWrite(self):
        return self.can_write

    def clearWriteFlag(self):
        self.can_write = False

    def size(self):
        return len(self.tags_dict)

    def add(self, tags, datapoint):
        if tags not in self.tags_dict:
            self.tags_dict[tags] = self.size() * self.points_num_max

        base_idx = self.tags_dict[tags]
        ts, val = datapoint

        # TODO: lock start_ts
        if self.start_ts is None:
            self.start_ts = ts - ts % self.resolution
            self.start_idx = base_idx
            offset = 0
        else:
            offset = (ts - self.start_ts) / self.resolution
        idx = base_idx + (self.start_idx + offset) % self.points_num_max

        if ts - self.start_ts - self.retention >= 0:  # 等待 30 秒
            self.can_write = True

        log.debug("add idx: %s, start_ts: %s, start_idx: %s" % (
                   idx, self.start_ts, self.start_idx))
        self.points[idx] = val

    def get_idx(self, timestamp):
        relative_idx = self.start_idx + (timestamp - self.start_ts) / self.resolution
        return relative_idx % self.points_num_max

    def align_timestamp(self, timestamp):
        return timestamp - (timestamp % self.resolution)

    def read(self, begin_ts=None, end_ts=None, clear=False):
        begin_ts = begin_ts or self.start_ts
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
        else:  # wrap around
            length = self.points_num_max - begin_idx + end_idx
            for i, (_, base_idx) in enumerate(self.tags_dict.items()):
                val = self.points[base_idx+begin_idx: base_idx+begin_idx+self.points_num_max]
                val += self.points[base_idx: base_idx+end_idx]
                rs[i] = val

        # empty tags
        for j in range(i+1, self.tags_num):
            rs[j] = [0] * length

        # timestamps
        begin_ts = self.align_timestamp(begin_ts)
        timestamps = [begin_ts + i * self.resolution for i in range(length)]

        # TODO: lock start_ts
        if clear:
            self.start_idx = end_idx
            self.start_ts = self.start_ts + self.resolution * length
            self.clearWriteFlag()
        return zip(timestamps, zip(*rs))


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
        self.size = 0
        self.lock = Lock()
        self.cache = self.initCache()

    def initCache(self):
        return {}

    def store(self, metric, tags, datapoint):
        log.msg("MetricCache received (%s, %s, %s)" % (metric, tags, datapoint))
        try:
            self.lock.acquire()
            if metric in self.cache:
                metric_data_list = self.cache[metric]
                flag = False
                for metric_data in metric_data_list:
                    if tags in metric_data.tags_dict:
                        flag = True
                        break

                if not flag and metric_data.full():
                    metric_data = MetricData()
                    metric_data_list.append(metric_data)
                metric_data.add(tags, datapoint)
            else:
                metric_data = MetricData()
                metric_data.add(tags, datapoint)
                self.cache[metric] = [metric_data]
        finally:
            self.lock.release()

    def pop(self, metric, metric_data_idx):
        try:
            self.lock.acquire()
            metric_data = self.cache[metric][metric_data_idx]
            datapoints = metric_data.read(clear=True)
            log.debug("canWrite: %s" % metric_data.canWrite())
            return datapoints
        finally:
            self.lock.release()

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

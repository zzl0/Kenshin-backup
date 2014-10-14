# coding: utf-8
import os
import shutil
import struct
import unittest

import kenshin.storage
from kenshin.storage import (
    Storage, METADATA_SIZE, METADATA_FORMAT, POINT_FORMAT, enable_debug)
from kenshin.utils import mkdir_p

MIN = 60
HOUR = 3600
DAY = HOUR * 24
WEEK = DAY * 7
YEAR = DAY * 365


class TestStorageIO(unittest.TestCase):
    data_dir = '/tmp/kenshin'

    def setUp(self):
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)

        mkdir_p(self.data_dir)
        self.storage = Storage(data_dir=self.data_dir)
        self.basic_setup = self._basic_setup()
        self.storage.create(*self.basic_setup)

        metric_name = self.basic_setup[0]
        self.path = self.storage.gen_path(self.data_dir, metric_name)

    def tearDown(self):
        shutil.rmtree(self.data_dir)

    def _basic_setup(self):
        metric_name = 'sys.cpu.user'

        tag_list = [
            'host=webserver01,cpu=0',
            'host=webserver01,cpu=1',
        ]
        archive_list = [
            (1, HOUR),
            (MIN, (2*DAY) / MIN),
            (5*MIN, WEEK / (5*MIN)),
            (15*MIN, (25*WEEK) / (15*MIN)),
            (12*HOUR, (5*YEAR) / (12*HOUR))
        ]
        x_files_factor = 40
        agg_name = 'min'
        return [metric_name, tag_list, archive_list, x_files_factor, agg_name]

    def _gen_val(self, i):
        return (i, 10+i)

    def test_io(self):
        """
        test io perfermance.

        (1000 io/s * 3600 s * 24) / (3*10**6 metric) / (30 metric/file) = 576 io/file
        由于 header 函数在一次写入中被调用了多次，而 header 数据较小，完全可以读取缓存数据，
        因此 enable_debug 中忽略了 header 的读操作。
        """
        enable_debug(ignore_header=True)

        now_ts = 1411628779
        ten_min = 10 * MIN
        from_ts = now_ts - DAY

        for i in range(DAY / ten_min):
            points = [(from_ts + i * ten_min + j, self._gen_val(i * ten_min + j))
                      for j in range(ten_min)]
            self.storage.update(self.path, points, from_ts + (i+1) * ten_min)

        open_ = kenshin.storage.open
        io = open_.read_cnt + open_.write_cnt
        io_limit = 576
        self.assertLessEqual(io, io_limit)

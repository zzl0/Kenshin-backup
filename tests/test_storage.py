# coding: utf-8
import os
import shutil
import struct
import unittest

from kenshin.storage import (
    Storage, METADATA_SIZE, METADATA_FORMAT, POINT_FORMAT)
from kenshin.agg import Agg
from kenshin.utils import mkdir_p, roundup


class TestStorage(unittest.TestCase):
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
            (1, 6),
            (3, 6),
        ]
        x_files_factor = 1.0
        agg_name = 'min'
        return [metric_name, tag_list, archive_list, x_files_factor, agg_name]

    def test_gen_path(self):
        metric_name = 'a.b.c'
        data_dir = '/x/y'
        path = self.storage.gen_path(data_dir, metric_name)
        self.assertEqual(path, '/x/y/a/b/c.hs')

    def test_header(self):
        metric_name, tag_list, archive_list, x_files_factor, agg_name = self.basic_setup
        with open(self.path, 'rb') as f:
            header = self.storage.header(f)

        self.assertEqual(tag_list, header['tag_list'])
        self.assertEqual(x_files_factor, header['x_files_factor'])
        self.assertEqual(Agg.get_agg_id(agg_name), header['agg_id'])

        _archive_list = [(x['sec_per_point'], x['count'])
                         for x in header['archive_list']]
        self.assertEqual(archive_list, _archive_list)

    def test_basic_update_fetch(self):
        now_ts = 1411628779
        num_points = 5
        points = [(now_ts - i, self._gen_val(i)) for i in range(1, num_points+1)]
        self.storage.update(self.path, points, now_ts)

        from_ts = now_ts - num_points
        series = self.storage.fetch(self.path, from_ts, now=now_ts)

        time_info = (from_ts, now_ts, 1)
        vals = [tuple(map(float, v)) for _, v in sorted(points)]
        expected = (time_info, vals)
        self.assertEqual(series[1:], expected)

    def _gen_val(self, i):
        return (i, 10+i)

    def test_update_propagate(self):
        now_ts = 1411628779
        num_points = 6
        points = [(now_ts - i, self._gen_val(i)) for i in range(1, num_points+1)]
        self.storage.update(self.path, points, now_ts)

        from_ts = now_ts - num_points - 1
        series = self.storage.fetch(self.path, from_ts, now=now_ts)
        time_info = (from_ts, roundup(now_ts, 3), 3)
        expected = time_info, [(5.0, 15.0), (2.0, 12.0), None]
        self.assertEqual(series[1:], expected)

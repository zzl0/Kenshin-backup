# coding: utf-8
import os
import shutil
import struct
import unittest

from kenshin.storage import (
    Storage, METADATA_SIZE, METADATA_FORMAT, POINT_FORMAT)
from kenshin.agg import Agg
from kenshin.utils import mkdir_p


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
            (1, 60),
            (60, 60),
        ]
        x_files_factor = 1.0
        agg_name = 'avg'
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
        def gen_val(i):
            return (10+i, 20+i)

        now_ts = 1411628779
        points = [(now_ts - i, gen_val(i)) for i in range(1, 6)]
        self.storage.update(self.path, points, now_ts)

        from_ts = now_ts - 5
        series = self.storage.fetch(self.path, from_ts, now=now_ts)

        time_info = (from_ts, now_ts, 1)
        vals = [tuple(map(float, v)) for _, v in sorted(points)]
        expected = (time_info, vals)
        self.assertEqual(series, expected)

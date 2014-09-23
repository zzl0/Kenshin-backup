# coding: utf-8
import os
import shutil
import struct
import unittest

from kenshin.storage import Storage, METADATA_SIZE, METADATA_FORMAT
from kenshin.agg import Agg
from kenshin.utils import mkdir_p


class TestStorage(unittest.TestCase):
    data_dir = '/tmp/kenshin'

    def setUp(self):
        mkdir_p(self.data_dir)
        self.storage = Storage(data_dir=self.data_dir)

    def tearDown(self):
        shutil.rmtree(self.data_dir)

    def test_create(self):
        metric_name = 'sys.cpu.user'
        tags = [
            'host=webserver01,cpu=0',
            'host=webserver01,cpu=1',
        ]
        archive_list = [
            (1, 60),
            (60, 60),
        ]
        x_files_factor = 1.0
        agg_name = 'avg'

        self.storage.create(metric_name, tags, archive_list, x_files_factor, agg_name)

        path = self.data_dir + '/sys/cpu/user.hs'
        with open(path, 'rb') as f:
            actural = f.read(METADATA_SIZE)
        expected = struct.pack(METADATA_FORMAT,
                               Agg.get_agg_id(agg_name),
                               3600,
                               x_files_factor,
                               len(archive_list),
                               len('\t'.join(tags)))
        self.assertEqual(actural, expected)

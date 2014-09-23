# coding: utf-8
#
# This module is an implementation of storage API. Here is the
# basic layout of fileformat.
#
# File = Header, Data
#     Header = Metadata, Tag+, ArchiveInfo+
#         Metadata = agg_id, max_retention, x_files_factor, archive_count, tag_size
#         Tag = type=value[,type2=value2]
#         ArchiveInfo = offset, seconds_per_point, point_count
#     Data =  Archive+
#         Archive = Point+
#             Point = timestamp, value
#

import os
import struct
import operator

from agg import Agg
from utils import mkdir_p


LONG_FORMAT = "!L"
LONG_SIZE = struct.calcsize(LONG_FORMAT)
FLOAT_FORMAT = "!f"
FLOAT_SIZE = struct.calcsize(FLOAT_FORMAT)
VALUE_FORMAT = "!d"
VALUE_SIZE = struct.calcsize(VALUE_FORMAT)
POINT_FORMAT = "!Ld"
POINT_SIZE = struct.calcsize(POINT_FORMAT)
METADATA_FORMAT = "!2Lf2L"
METADATA_SIZE = struct.calcsize(METADATA_FORMAT)
ARCHIVEINFO_FORMAT = "!3L"
ARCHIVEINFO_SIZE = struct.calcsize(ARCHIVEINFO_FORMAT)


class Storage(object):

    def __init__(self, data_dir):
        self.data_dir = data_dir

    def create(self, metric_name, tags, archive_list, x_files_factor=None,
               agg_name=None):
        path = self._gen_path(metric_name)

        if os.path.exists(path):
            raise IOError('file %s already exits.' % path)
        else:
            mkdir_p(os.path.dirname(path))

        with open(path, 'wb') as f:
            packed_header, end_offset = self._pack_header(
                                            tags, archive_list, x_files_factor,
                                            agg_name)
            f.write(packed_header)

            # init data
            remaining = end_offset - f.tell()
            chunk_size = 16384
            zeroes = '\x00' * chunk_size
            while remaining > chunk_size:
                f.write(zeroes)
                remaining -= chunk_size
            f.write(zeroes[:remaining])

    def _gen_path(self, metric_name):
        """
        Generate file path of `metric_name`.

        eg, metric_name is `sys.cpu.user`, the absolute file path will be
        `self.data_dir/sys/cpu/user.hs`
        """
        parts = metric_name.split('.')
        parts[-1] = parts[-1] + '.hs'
        file_path = os.path.sep.join(parts)
        return os.path.join(self.data_dir, file_path)

    def _pack_header(self, tags, archive_list, x_files_factor, agg_name):
        # tag
        tag = '\t'.join(tags)

        # metadata
        agg_id = struct.pack(LONG_FORMAT, Agg.get_agg_id(agg_name))
        max_retention = struct.pack(
            LONG_FORMAT,
            reduce(operator.mul, archive_list[-1], 1))
        xff = struct.pack(FLOAT_FORMAT, x_files_factor)
        archive_cnt = struct.pack(LONG_FORMAT, len(archive_list))
        tag_size = struct.pack(LONG_FORMAT, len(tag))
        metadata = ''.join([agg_id, max_retention, xff, archive_cnt, tag_size])

        # archive_info
        header = [metadata, tag]
        offset = METADATA_SIZE + len(tag) + ARCHIVEINFO_SIZE * len(archive_list)
        tag_cnt = len(tags)

        for sec, cnt in archive_list:
            archive_info = struct.pack(ARCHIVEINFO_FORMAT, offset, sec, cnt)
            header.append(archive_info)
            offset += POINT_SIZE * cnt * tag_cnt
        return ''.join(header), offset

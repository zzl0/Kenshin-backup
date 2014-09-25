# coding: utf-8
#
# This module is an implementation of storage API. Here is the
# basic layout of fileformat.
#
# File = Header, Data
#     Header = Metadata, Tag+, ArchiveInfo+
#         Metadata = agg_id, max_retention, x_files_factor, archive_count, tag_size, point_size
#         Tag = type=value[,type2=value2]
#         ArchiveInfo = offset, seconds_per_point, point_count
#     Data = Archive+
#         Archive = Point+
#             Point = timestamp, value
#

import os
import time
import struct
import operator

from agg import Agg
from utils import mkdir_p, roundup


LONG_FORMAT = "!L"
LONG_SIZE = struct.calcsize(LONG_FORMAT)
FLOAT_FORMAT = "!f"
FLOAT_SIZE = struct.calcsize(FLOAT_FORMAT)
VALUE_FORMAT = "!d"
VALUE_SIZE = struct.calcsize(VALUE_FORMAT)
POINT_FORMAT = "!L%dd"
# POINT_SIZE = struct.calcsize(POINT_FORMAT)
METADATA_FORMAT = "!2Lf3L"
METADATA_SIZE = struct.calcsize(METADATA_FORMAT)
ARCHIVEINFO_FORMAT = "!3L"
ARCHIVEINFO_SIZE = struct.calcsize(ARCHIVEINFO_FORMAT)


### Exceptioons

class KenshinException(Exception):
    pass


class InvalidTime(KenshinException):
    pass


### Storage

class Storage(object):

    def __init__(self, data_dir):
        self.data_dir = data_dir

    def create(self, metric_name, tag_list, archive_list, x_files_factor=None,
               agg_name=None):
        path = self.gen_path(self.data_dir, metric_name)

        if os.path.exists(path):
            raise IOError('file %s already exits.' % path)
        else:
            mkdir_p(os.path.dirname(path))

        with open(path, 'wb') as f:
            packed_header, end_offset = self._pack_header(
                tag_list, archive_list, x_files_factor, agg_name)
            f.write(packed_header)

            # init data
            remaining = end_offset - f.tell()
            chunk_size = 16384
            zeroes = '\x00' * chunk_size
            while remaining > chunk_size:
                f.write(zeroes)
                remaining -= chunk_size
            f.write(zeroes[:remaining])

    @staticmethod
    def gen_path(data_dir, metric_name):
        """
        Generate file path of `metric_name`.

        eg, metric_name is `sys.cpu.user`, the absolute file path will be
        `self.data_dir/sys/cpu/user.hs`
        """
        parts = metric_name.split('.')
        parts[-1] = parts[-1] + '.hs'
        file_path = os.path.sep.join(parts)
        return os.path.join(data_dir, file_path)

    def _pack_header(self, tag_list, archive_list, x_files_factor, agg_name):
        # tag
        tag = '\t'.join(tag_list)

        # metadata
        agg_id = Agg.get_agg_id(agg_name)
        max_retention = reduce(operator.mul, archive_list[-1], 1)
        xff = x_files_factor
        archive_cnt = len(archive_list)
        tag_size = len(tag)
        point_size = struct.calcsize(POINT_FORMAT % len(tag_list))
        metadata = struct.pack(METADATA_FORMAT, agg_id, max_retention,
            xff, archive_cnt, tag_size, point_size)

        # archive_info
        header = [metadata, tag]
        offset = METADATA_SIZE + len(tag) + ARCHIVEINFO_SIZE * len(archive_list)

        for sec, cnt in archive_list:
            archive_info = struct.pack(ARCHIVEINFO_FORMAT, offset, sec, cnt)
            header.append(archive_info)
            offset += point_size * cnt
        return ''.join(header), offset

    @staticmethod
    def header(fh):
        origin_offset = fh.tell()
        if origin_offset != 0:
            fh.seek(0)
        packed_metadata = fh.read(METADATA_SIZE)
        agg_id, max_retention, xff, archive_cnt, tag_size, point_size = struct.unpack(
            METADATA_FORMAT, packed_metadata)
        tag_list = fh.read(tag_size).split('\t')

        archives = []
        for i in xrange(archive_cnt):
            packed_archive_info = fh.read(ARCHIVEINFO_SIZE)
            offset, sec, cnt = struct.unpack(
                ARCHIVEINFO_FORMAT, packed_archive_info)
            archive_info = {
                'offset': offset,
                'sec_per_point': sec,
                'count': cnt,
                'size': point_size * cnt,
                'retention': sec * cnt,
            }
            archives.append(archive_info)

        fh.seek(origin_offset)
        info = {
            'agg_id': agg_id,
            'max_retention': max_retention,
            'x_files_factor': xff,
            'tag_list': tag_list,
            'point_size': point_size,
            'point_format': POINT_FORMAT % len(tag_list),
            'archive_list': archives,
        }
        return info

    def update(self, path, points, now=None):
        # order points by timestamp, newest first
        points.sort(key=operator.itemgetter(0), reverse=True)
        with open(path, 'r+b') as f:
            header = self.header(f)
            if now is None:
                now = int(time.time())
            archive_list = iter(header['archive_list'])
            first_archive = header['archive_list'][0]
            first_retention = first_archive['retention']

            # remove outdated points
            curr_points = [p for p in points
                           if first_retention > (now-p[0])]
            self._update_archive(f, header, first_archive, curr_points)

    def _update_archive(self, fh, header, archive, points):
        time_step = archive['sec_per_point']
        aligned_points = [(ts - (ts % time_step), val)
                          for (ts, val) in points]

        # take the last val of duplicates
        aligned_points = sorted(dict(aligned_points).items(),
                                key=operator.itemgetter(0))

        # create packed string
        point_format = header['point_format']
        point_size = header['point_size']
        packed_str = ''.join(struct.pack(point_format, ts, *val)
                             for ts, val in aligned_points)

        # read base point and determine where our writes will start
        base_point = self._read_base_point(fh, archive, header)
        base_ts = base_point[0]

        first_ts = aligned_points[0][0]
        if base_ts == 0:
            # this file's first update, so set it to first timestamp
            base_ts = first_ts

        offset = self._timestamp2offset(first_ts, base_ts, header, archive)
        archive_end = archive['offset'] + archive['size']
        bytes_beyond = (offset + len(packed_str)) - archive_end

        fh.seek(offset)
        if bytes_beyond > 0:
            fh.write(packed_str[:-bytes_beyond])
            fh.seek(archive['offset'])
            fh.write(packed_str[-bytes_beyond:])
        else:
            fh.write(packed_str)

    def _read_base_point(self, fh, archive, header):
        fh.seek(archive['offset'])
        base_point = fh.read(header['point_size'])
        return struct.unpack(header['point_format'], base_point)

    def _timestamp2offset(self, ts, base_ts, header, archive):
        time_distance = ts - base_ts
        point_distince = time_distance / archive['sec_per_point']
        byte_distince =  point_distince * header['point_size']
        return archive['offset'] + (byte_distince % archive['size'])

    def fetch(self, path, from_time, until_time=None, now=None):
        with open(path, 'rb') as f:
            header = self.header(f)

            # validate timestamp
            if now is None:
                now = int(time.time())
            if until_time is None:
                until_time = now
            if from_time > until_time:
                raise InvalidTime("from_time '%s' is after unitl_time '%s'" %
                                  (from_time, unitl_time))

            oldest_time = now - header['max_retention']
            if from_time > now:
                return None
            if until_time < oldest_time:
                return None

            until_time = min(now, until_time)
            from_time = max(oldest_time, from_time)

            diff = now - from_time
            for archive in header['archive_list']:
                if archive['retention'] >= diff:
                    break

            return self._archive_fetch(f, header, archive, from_time, until_time)


    def _archive_fetch(self, fh, header, archive, from_time, until_time):
        from_time = roundup(from_time, archive['sec_per_point'])
        until_time = roundup(until_time, archive['sec_per_point'])

        base_point = self._read_base_point(fh, archive, header)
        base_ts = base_point[0]

        if base_ts == 0:
            step = archive['sec_per_point']
            cnt = (until_time - from_time) / step
            time_info = (from_time, until_time, step)
            val_list = [None] * cnt
            return (time_info, val_list)

        from_offset = self._timestamp2offset(from_time, base_ts, header, archive)
        until_offset = self._timestamp2offset(until_time, base_ts, header, archive)

        fh.seek(from_offset)
        if from_offset < until_offset:
            series_str = fh.read(until_offset - from_offset)
        else:
            archive_end = archive['offset'] + archive['size']
            series_str = fh.read(archive_end - from_offset)
            fh.seek(archive['offset'])
            series_str += fh.read(until_offset - archive['offset'])

        ## unpack series string
        point_format = header['point_format']
        byte_order, point_type = point_format[0], point_format[1:]
        cnt = len(series_str) / header['point_size']
        series_format = byte_order + point_type * cnt
        unpacked_series = struct.unpack(series_format, series_str)

        ## construct value list
        # pre-allocate entire list or speed
        tag_cnt = len(header['tag_list'])
        val_list = [None] * cnt
        step = tag_cnt + 1
        for i in xrange(0, len(unpacked_series), step):
            val = unpacked_series[i+1: i+step]
            val_list[i/step] = val

        time_info = (from_time, until_time, archive['sec_per_point'])
        return time_info, val_list

# coding: utf-8

import struct


longFormat = "!L"
longSize = struct.calcsize(longFormat)
floatFormat = "!f"
floatSize = struct.calcsize(floatFormat)
valueFormat = "!d"
valueSize = struct.calcsize(valueFormat)
pointFormat = "!Ld"
pointSize = struct.calcsize(pointFormat)
metadataFormat = "!2LfL"
metadataSize = struct.calcsize(metadataFormat)
archiveInfoFormat = "!3L"
archiveInfoSize = struct.calcsize(archiveInfoFormat)

agg_type_dict = dict({
  1: 'average',
  2: 'sum',
  3: 'last',
  4: 'max',
  5: 'min'
})


def get_agg_name(agg_id):
    return agg_type_dict[agg_id]


def read_header(filename):
    with open(filename) as fh:
        packed_meta = fh.read(metadataSize)
        try:
            agg_type, max_ret, xff, archive_cnt = struct.unpack(
                metadataFormat, packed_meta)
        except:
            raise Exception("Unable to read header", filename)

        archives = []
        for i in xrange(archive_cnt):
            packed_archive_info = fh.read(archiveInfoSize)
            try:
                off, sec, cnt = struct.unpack(archiveInfoFormat, packed_archive_info)
            except:
                raise Exception(
                    "Unable to read archive%d metadata" % i, filename)

            archive_info = {
                'offset': off,
                'sec_per_point': sec,
                'count': cnt,
                'size': pointSize * cnt,
                'retention': sec * cnt,
            }
            archives.append(archive_info)

        info = {
            'xff': xff,
            'archives': archives,
            'agg_type': agg_type,
        }
        return info

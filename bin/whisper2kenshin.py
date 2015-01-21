# coding: utf-8
import os
import sys
import time
import struct
import string
import fnv1a
from multiprocessing import Process, Queue

from kenshin import storage
from kenshin.consts import NULL_VALUE
from kenshin.storage import Storage
from kenshin.tools.whisper_tool import (read_header, metadataSize,
    pointFormat, pointSize, get_agg_name)
from kenshin.tools.hash import Hash
from kenshin.utils import mkdir_p
from rurouni.storage import loadStorageSchemas


ID, META, METRICS, INDEX_FH = range(4)


def flatten(iterable):
    """ Recursively iterate lists and tuples.

    >>> list(flatten([1, (2, 3, [4]), 5]))
    [1, 2, 3, 4, 5]
    """
    for elm in iterable:
        if isinstance(elm, (list, tuple)):
            for relm in flatten(elm):
                yield relm
        else:
            yield elm


def merge_points(whisper_points, needed_metrics=0):
    """ Merge whisper points to a kenshin point.

    >>> whisper_points = [
    ...   [[1421830133, 0], [1421830134, 1], [1421830135, 2]],
    ...   [[1421830134, 4], [1421830135, 5]],
    ...   [[1421830133, 6], [1421830134, 7], [1421830135, 8]]
    ... ]
    >>> merge_points(whisper_points)
    [(1421830133, [0, -4294967296.0, 6]), (1421830134, [1, 4, 7]), (1421830135, [2, 5, 8])]
    """
    length = len(whisper_points) + needed_metrics
    d = {}
    for i, points in enumerate(whisper_points):
        for t, v in points:
            if not t:
                continue
            if t not in d:
                d[t] = [NULL_VALUE] * length
            d[t][i] = v
    return sorted(d.items())


def read_whisper_points(content, archive, now):
    off, size, cnt = archive['offset'], archive['size'], archive['count']
    packed_bytes = content[off: off+size]
    point_format = pointFormat[0] + pointFormat[1:] * cnt
    points = struct.unpack(point_format, packed_bytes)
    ts_limit = now - archive['retention']
    return [(points[i], points[i+1]) for i in range(0, len(points), 2)
            if points[i] >= ts_limit]


def fill_gap(archive_points, archive, metrics_max_num):
    EMPTY_POINT = (0, (0,) * metrics_max_num)
    if not archive_points:
        return [EMPTY_POINT] * archive['count']
    step = archive['sec_per_point']
    rs = [archive_points[0]]
    last_ts = archive_points[0][0]
    for ts, point in archive_points[1:]:
        if last_ts + step == ts:
            rs.append((ts, point))
        else:
            rs.extend([EMPTY_POINT] * ((ts - last_ts) / step))
        last_ts = ts
    if len(rs) < archive['count']:
        rs.extend([EMPTY_POINT] * (archive['count'] - len(rs)))
    else:
        rs = rs[:archive['count']]
    return rs


def packed_kenshin_points(points):
    point_format = storage.POINT_FORMAT % len(points[0][1])
    str_format = point_format[0] + point_format[1:] * len(points)
    return struct.pack(str_format, *flatten(points))


def gen_output_file(id, meta, output_dir):
    return os.path.sep.join([output_dir, meta['instance'], str(id)]) + '.hs'


def merge_files(id, meta, metrics, data_dir, output_dir):
    contents = []
    for m in metrics:
        filename = metric_to_filepath(m, data_dir)
        with open(filename) as f:
            contents.append(f.read())
    output_file = gen_output_file(id, meta, output_dir)
    mkdir_p(os.path.dirname(output_file))
    needed_metrics = meta['metrics_max_num'] - len(metrics)
    now = int(time.time())

    with open(output_file, 'w') as f:
        archives = meta['archives']
        archive_info = [(archive['sec_per_point'], archive['count'])
                        for archive in archives]
        agg_name = get_agg_name(meta['agg_type'])
        inter_tag_list = metrics + [''] * (needed_metrics + 1)

        # header
        packed_kenshin_header = Storage.pack_header(
            inter_tag_list, archive_info, meta['xff'],
            agg_name)[0]
        f.write(packed_kenshin_header)

        # archives
        for archive in archives:
            whisper_points = [read_whisper_points(content, archive, now)
                              for content in contents]
            archive_points = merge_points(whisper_points, needed_metrics)
            archive_points = fill_gap(archive_points, archive, meta['metrics_max_num'])
            packed_str = packed_kenshin_points(archive_points)
            f.write(packed_str)


def metric_to_filepath(metric, data_dir):
    return os.path.sep.join([data_dir] + metric.split('.')) + '.wsp'


def worker(queue):
    for (id, meta, metrics, data_dir, output_dir) in iter(queue.get, 'STOP'):
        merge_files(id, meta, metrics, data_dir, output_dir)
    return True


def get_queue_item(val, data_dir, output_dir):
    return val[ID], val[META], val[METRICS], data_dir, output_dir


def write_to_index(val):
    id = val[ID]
    fh = val[INDEX_FH]
    for i, m in enumerate(val[METRICS]):
        fh.write("%s %s %s\n" % (m, id, i))


def get_schema(storage_schemas, metric):
    for schema in storage_schemas:
        if schema.match(metric):
            return schema


def get_instance(metric, instances):
    idx = fnv1a.get_hash_bugfree(metric) % instances
    assert instances <= 26
    return string.lowercase[idx]


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", required=True, help="rurouni config file.")
    parser.add_argument("-o", "--output_dir", required=True, help="output directory.")
    parser.add_argument("-d", "--data_dir", required=True, help="whisper file data directory.")
    parser.add_argument("-m", "--metrics_file", required=True, help="metrics that we needed.")
    parser.add_argument("-n", "--instances", type=int, default=2, help="number of instances.")
    parser.add_argument("-p", "--processes", type=int, default=10, help="number of processes.")
    args = parser.parse_args()

    new_metrics_schemas = {}  # {(instance, schema_name): [id, meta, [metric, ...], index_fh]}
    storage_schemas = loadStorageSchemas(args.config)
    blacklist = {'stats-counters-count',}

    queue = Queue()

    with open(args.metrics_file) as f:
        for line in f:
            metric = line.strip()
            schema = get_schema(storage_schemas, metric)
            if schema.name in blacklist:
                continue
            instance = get_instance(metric, args.instances)

            key = (instance, schema.name)
            new_metrics_schemas.setdefault(key, [0, None, [], None])
            if not new_metrics_schemas[key][META]:
                meta = read_header(metric_to_filepath(metric, args.data_dir))
                meta["instance"] = instance
                meta["metrics_max_num"] = schema.metrics_max_num
                new_metrics_schemas[key][META] = meta

            if not new_metrics_schemas[key][INDEX_FH]:
                index_file = os.path.join(args.output_dir, instance + '.idx')
                new_metrics_schemas[key][INDEX_FH] = open(index_file, "w")
            new_metrics_schemas[key][METRICS].append(metric)

            if len(new_metrics_schemas[key][METRICS]) == schema.metrics_max_num:
                val = new_metrics_schemas[key]
                item = get_queue_item(val, args.data_dir, args.output_dir)
                queue.put(item)
                write_to_index(val)

                new_metrics_schemas[key][META] = []
                new_metrics_schemas[key][ID] += 1

        for key, val in new_metrics_schemas.items():
            if len(val[METRICS]):
                item = get_queue_item(val, args.data_dir, args.output_dir)
                queue.put(item)
                write_to_index(val)

    processes = []
    for w in xrange(args.processes):
        p = Process(target=worker, args=(queue,))
        p.start()
        processes.append(p)
        queue.put("STOP")

    for p in processes:
        p.join()

    for key in new_metrics_schemas:
        new_metrics_schemas[key][INDEX_FH].close()


if __name__ == '__main__':
    main()

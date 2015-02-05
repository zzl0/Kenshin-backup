# coding: utf-8
import os
import sys
import time
import struct
import string
import fnv1a
from multiprocessing import Process, Queue
from ConfigParser import ConfigParser

from kenshin import storage
from kenshin.consts import NULL_VALUE
from kenshin.storage import Storage
from kenshin.tools.whisper_tool import (read_header, metadataSize,
    pointFormat, pointSize, get_agg_name, gen_whisper_schema_func)
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
    return os.path.join(output_dir, meta['instance'],
                        meta['schema_name'], str(id)+'.hs')


def merge_files(meta, metrics, data_dir, output_file):
    contents = []
    for m in metrics:
        filename = metric_to_filepath(m, data_dir)
        with open(filename) as f:
            contents.append(f.read())
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


def gen_links(metrics, data_path, link_dir, instance):
    for m in metrics:
        link_path = m.replace('.', os.path.sep)
        link_path = os.path.join(link_dir, instance, link_path + '.hs')
        dirname = os.path.dirname(link_path)
        mkdir_p(dirname)
        if os.path.exists(link_path):
            os.remove(link_path)
        os.symlink(data_path, link_path)


def worker(queue):
    for (id, meta, metrics, data_dir, output_dir, link_dir) in iter(queue.get, 'STOP'):
        output_file = gen_output_file(id, meta, output_dir)
        try:
            merge_files(meta, metrics, data_dir, output_file)
            gen_links(metrics, output_file, link_dir, meta['instance'])
        except Exception as e:
            print >>sys.stderr, '[merge error] %s: metrics[0]=%s' % (e, metrics[0])
            if os.path.exists(output_file):
                os.remove(output_file)
    return True


def get_queue_item(val, data_dir, output_dir, link_dir):
    return val[ID], val[META], val[METRICS], data_dir, output_dir, link_dir


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


def parse_rurouni_config(conf):
    """ Return rurouni instance.
    {instance: {"local_data_dir": "", "local_link_dir": ""}}
    """
    parser = ConfigParser()
    parser.read(conf)
    cache_sections = [x for x in parser.sections() if x.startswith('cache')]
    keys = {'local_data_dir', 'local_link_dir'}
    rs = {}
    for section in cache_sections:
        parts = section.split(':')
        val = dict((k, v) for (k, v) in parser.items('cache') if k in keys)
        if len(parts) == 1:
            instance = 'a'
            val_2 = {}
        else:
            instance = parts[1]
            val_2 = dict((k, v) for (k, v) in parser.items(section) if k in keys)
        val.update(val_2)
        rs[instance] = val
    return rs


def skip_metric(metric, metric_data_path, kenshin_schema, whisper_schema):
    blacklist = {'stats-counters-count',}
    flag = False
    key_info = '(%s, %s, %s)' % (metric, kenshin_schema.name,
                             whisper_schema.name)
    reason_pat = '[schema error] %s: ' + key_info
    if kenshin_schema.name in blacklist:
        flag = True
        reason = reason_pat % 'schema name in blacklist'
    elif not os.path.exists(metric_data_path):
        flag = True
        reason = reason_pat % 'data file not exists'
    elif kenshin_schema.aggregationMethod != whisper_schema.aggregationMethod:
        flag = True
        reason = reason_pat % 'aggregation method not match'
    elif len(kenshin_schema.archives) != len(whisper_schema.archives):
        flag = True
        reason = reason_pat % "archives length not match"
    else:
        for i in range(len(kenshin_schema.archives)):
            if kenshin_schema.archives[i] != whisper_schema.archives[i]:
                flag = True
                reason = reason_pat % ("archive(%d) not match %s %s" %
                                       (i, kenshin_schema.archives[i],
                                        whisper_schema.archives[i]))
                break
    if flag:
        print >>sys.stderr, reason
    return flag


def gen_index_file_handlers(instances_info):
    rs = {}
    for instance in instances_info:
        data_dir = instances_info[instance]['local_data_dir']
        index_file = os.path.join(data_dir, instance + '.idx')
        rs[instance] = open(index_file, 'w')
    return rs


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--kenshin_conf_dir", required=True, help="kenshin conf directory.")
    parser.add_argument("--whisper_conf_dir", required=True, help="whisper conf directory.")
    parser.add_argument("-d", "--data_dir", required=True, help="whisper file data directory.")
    parser.add_argument("-m", "--metrics_file", required=True, help="metrics that we needed.")
    parser.add_argument("-p", "--processes", type=int, default=10, help="number of processes.")
    args = parser.parse_args()

    rurouni_conf = os.path.join(args.kenshin_conf_dir, 'rurouni.conf')
    instances_info =  parse_rurouni_config(rurouni_conf)
    index_file_handlers = gen_index_file_handlers(instances_info)

    kenshin_storage_conf = os.path.join(args.kenshin_conf_dir, 'storage-schemas.conf')
    kenshin_storage_schemas = loadStorageSchemas(kenshin_storage_conf)
    get_whisper_schema = gen_whisper_schema_func(args.whisper_conf_dir)

    new_metrics_schemas = {}  # {(instance, schema_name): [id, meta, [metric, ...], index_fh]}
    queue = Queue()

    processes = []
    for w in xrange(args.processes):
        p = Process(target=worker, args=(queue,))
        p.start()
        processes.append(p)

    with open(args.metrics_file) as f:
        for line in f:
            metric = line.strip()
            schema = get_schema(kenshin_storage_schemas, metric)
            whisper_schema = get_whisper_schema(metric)
            metric_data_path = metric_to_filepath(metric, args.data_dir)

            if skip_metric(metric, metric_data_path, schema, whisper_schema):
                continue

            instance = get_instance(metric, len(instances_info))
            output_dir = instances_info[instance]['local_data_dir']
            link_dir = instances_info[instance]['local_link_dir']
            key = (instance, schema.name)
            # value is: [ID, META, METRICS, INDEX_FH]
            new_metrics_schemas.setdefault(key, [0, None, [], None])

            # set meta
            if not new_metrics_schemas[key][META]:
                meta = read_header(metric_data_path)
                meta["instance"] = instance
                meta["metrics_max_num"] = schema.metrics_max_num
                meta["schema_name"] = schema.name
                meta["xff"] = schema.xFilesFactor
                new_metrics_schemas[key][META] = meta

            # set index file handler
            if not new_metrics_schemas[key][INDEX_FH]:
                new_metrics_schemas[key][INDEX_FH] = index_file_handlers[instance]

            new_metrics_schemas[key][METRICS].append(metric)

            # add a group of metrics to the queue
            if len(new_metrics_schemas[key][METRICS]) == schema.metrics_max_num:
                val = new_metrics_schemas[key]
                item = get_queue_item(val, args.data_dir, output_dir, link_dir)
                queue.put(item)
                write_to_index(val)
                new_metrics_schemas[key][METRICS] = []
                new_metrics_schemas[key][ID] += 1

        for (instance, _), val in new_metrics_schemas.items():
            output_dir = instances_info[instance]['local_data_dir']
            link_dir = instances_info[instance]['local_link_dir']
            if len(val[METRICS]):
                item = get_queue_item(val, args.data_dir, output_dir, link_dir)
                queue.put(item)
                write_to_index(val)

    for _ in xrange(args.processes):
        queue.put("STOP")

    for p in processes:
        p.join()

    for _, fh in index_file_handlers.items():
        fh.close()


if __name__ == '__main__':
    main()

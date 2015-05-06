#!/usr/bin/env python
# coding: utf-8
import os
import time
import glob

import kenshin
from kenshin.consts import NULL_VALUE
from kenshin.utils import get_metric as _get_metric
from kenshin.agg import Agg
from rurouni.storage import loadStorageSchemas


def parse_rurouni_config(conf):
    """ Return rurouni instance.
    {instance: {"local_data_dir": "", "local_link_dir": ""}}
    """
    # TODO: this copied from whisper2kenshin.py
    from ConfigParser import ConfigParser
    parser = ConfigParser()
    parser.read(conf)
    cache_sections = [x for x in parser.sections() if x.startswith('cache')]
    keys = set(['local_data_dir', 'local_link_dir'])
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


def get_metric(metric_or_path):
    if metric_or_path.endswith('.hs'):
        return _get_metric(metric_or_path)
    else:
        return metric_or_path


def get_metric_path(metric, data_dirs):
    def metric_path(metric, data_dir):
        pat = os.path.join(data_dir, '*', metric.replace('.', '/')) + '.hs'
        rs = glob.glob(pat)
        assert len(rs) <= 1
        if rs:
            return rs[0]

    rs = [metric_path(metric, d) for d in data_dirs
          if metric_path(metric, d)]
    assert len(rs) == 1
    return os.path.realpath(rs[0])


def get_schema(storage_schemas, metric):
    for schema in storage_schemas:
        if schema.match(metric):
            return schema


def resize_metric(metric, schema, data_dirs):
    rebuild = False
    msg = ""

    path = get_metric_path(metric, data_dirs)
    print path
    with open(path) as f:
        header = kenshin.header(f)
    retentions = schema.archives
    old_retentions = [(r['sec_per_point'], r['count'])
                      for r in header['archive_list']]

    if retentions != old_retentions:
        rebuild = True
        msg += "retentions:\n%s -> %s" % (retentions, old_retentions)

    if not rebuild:
        print 'No Operation Needed.'
    else:
        print msg
        now = int(time.time())

        tmpfile = path + '.tmp'
        if os.path.exists(tmpfile):
            print 'Removing previous temporary database file: %s' % tmpfile
            os.unlink(tmpfile)

        print 'Creating new kenshin database: %s' % tmpfile
        kenshin.create(tmpfile,
                       [''] * len(header['tag_list']),
                       schema.archives,
                       header['x_files_factor'],
                       Agg.get_agg_name(header['agg_id']))
        for i, t in enumerate(header['tag_list']):
            kenshin.add_tag(t, tmpfile, i)

        size = os.stat(tmpfile).st_size
        old_size = os.stat(tmpfile).st_size
        print 'Created: %s (%d bytes, was %d bytes)' % (
              tmpfile, size, old_size)

        print 'Migrating data to new kenshin database ...'
        for archive in header['archive_list']:
            from_time = now - archive['retention'] + archive['sec_per_point']
            until_time = now
            _, timeinfo, values = kenshin.fetch(path, from_time, until_time)
            datapoints = zip(range(*timeinfo), values)
            datapoints = [[p[0], list(p[1])] for p in datapoints if p[1]]
            for ts, values in datapoints:
                for i, v in enumerate(values):
                    if v is None:
                        values[i] = NULL_VALUE
            kenshin.update(tmpfile, datapoints)

        backup = path + '.bak'
        print 'Renaming old database to: %s' % backup
        os.rename(path, backup)

        print 'Renaming new database to: %s' % path
        try:
            os.rename(tmpfile, path)
        except:
            os.rename(backup, path)
            raise IOError('Operation failed, restoring backup')

        # Notice: by default, '.bak' files are not deleted.


def main():
    usage = ("Usage: kenshin-resize.py [metric|path]\n"
             "Note: kenshin combined many metrics to one file, "
             "      please make sure you want to resize the file. "
             "      (use keshin-info.py to view the file meta data)")

    import argparse
    parser = argparse.ArgumentParser(description=usage,
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument(
        "--kenshin_conf_dir", required=True, help="kenshin conf directory.")
    parser.add_argument(
        "--metric", required=True, help="metric or metric path.")
    args = parser.parse_args()

    metric = get_metric(args.metric)
    storage_conf_path = os.path.join(args.kenshin_conf_dir, 'storage-schemas.conf')
    storage_schemas = loadStorageSchemas(storage_conf_path)
    schema = get_schema(storage_schemas, metric)

    rurouni_conf_path = os.path.join(args.kenshin_conf_dir, 'rurouni.conf')
    rurouni_conf = parse_rurouni_config(rurouni_conf_path)
    data_dirs = set(v['local_link_dir'] for (k,v) in rurouni_conf.iteritems())
    resize_metric(metric, schema, data_dirs)


if __name__ == '__main__':
    main()

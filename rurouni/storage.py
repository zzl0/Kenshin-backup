# coding: utf-8

import os
import re
from os.path import join, sep

import kenshin
from kenshin.utils import mkdir_p
from rurouni import log
from rurouni.conf import settings, OrderedConfigParser


def getFilePath(schema_name, file_idx):
    return join(settings.LOCAL_DATA_DIR, settings['instance'],
                schema_name, '%d.hs' % file_idx)


def getMetricPath(metric):
    path = metric.replace('.', sep)
    return join(settings.LOCAL_LINK_DIR, settings['instance'], path + '.hs')


def createLink(metric, file_path):
    metric_path = getMetricPath(metric)
    dir_name = os.path.dirname(metric_path)
    mkdir_p(dir_name)
    if os.path.lexists(metric_path):
        os.rename(metric_path, metric_path+'.bak')
    os.symlink(file_path, metric_path)


class Archive:
    def __init__(self, secPerPoint, points):
        self.secPerPoint = secPerPoint
        self.points = points

    def __str__(self):
        return 'Archive(%s, %s)' % (self.secPerPoint, self.points)

    def getTuple(self):
        return self.secPerPoint, self.points

    @staticmethod
    def fromString(retentionDef):
        rs = kenshin.parse_retention_def(retentionDef)
        return Archive(*rs)


class Schema(object):
    def match(self, metric):
        raise NotImplementedError()


class DefaultSchema(Schema):
    def __init__(self, name, xFilesFactor, aggregationMethod, archives,
                 cache_retention, metrics_max_num, cache_ratio):
        self.name = name
        self.xFilesFactor = xFilesFactor
        self.aggregationMethod = aggregationMethod
        self.archives = archives
        self.cache_retention = cache_retention
        self.metrics_max_num = metrics_max_num
        self.cache_ratio = cache_ratio

    def match(self, metric):
        return True


class PatternSchema(Schema):
    def __init__(self, name, pattern, xFilesFactor, aggregationMethod, archives,
                 cache_retention, metrics_max_num, cache_ratio):
        self.name = name
        self.pattern = re.compile(pattern)
        self.xFilesFactor = xFilesFactor
        self.aggregationMethod = aggregationMethod
        self.archives = archives
        self.cache_retention = cache_retention
        self.metrics_max_num = metrics_max_num
        self.cache_ratio = cache_ratio

    def match(self, metric):
        return self.pattern.match(metric)


def loadStorageSchemas(conf_file):
    schema_list = []
    config = OrderedConfigParser()
    config.read(conf_file)

    for section in config.sections():
        options = dict(config.items(section))

        pattern = options.get('pattern')
        xff = float(options.get('xfilesfactor'))
        agg = options.get('aggregationmethod')
        retentions = options.get('retentions').split(',')
        archives = [Archive.fromString(s).getTuple() for s in retentions]
        cache_retention = kenshin.RetentionParser.parse_time_str(
            options.get('cacheretention'))
        metrics_max_num = options.get('metricsperfile')
        cache_ratio = 1.2

        try:
            kenshin.validate_archive_list(archives, xff)
        except kenshin.InvalidConfig as e:
            log.err("Invalid schema found in %s." % section)

        schema = PatternSchema(section, pattern, float(xff), agg, archives,
                               int(cache_retention), int(metrics_max_num),
                               float(cache_ratio))
        schema_list.append(schema)
    schema_list.append(defaultSchema)
    return schema_list


# default schema

defaultSchema = DefaultSchema(
    'default',
    1.0,
    'avg',
    ((60, 60 * 24 * 7)),  # default retention (7 days of minutely data)
    600,
    40,
    1.2
)


class StorageSchemas(object):
    def __init__(self, conf_file):
        self.schemas = loadStorageSchemas(conf_file)

    def getSchemaByMetric(self, metric):
        for schema in self.schemas:
            if schema.match(metric):
                return schema
        return defaultSchema

    def getSchemaByName(self, schema_name):
        for schema in self.schemas:
            if schema.name == schema_name:
                return schema
        return None


if __name__ == '__main__':
    loadStorageSchemas()

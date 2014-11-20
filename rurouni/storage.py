# coding: utf-8

import os
import re
from os.path import join, sep
from ConfigParser import ConfigParser
from collections import OrderedDict

import kenshin
from rurouni import log
from rurouni.conf import settings, OrderedConfigParser


def getFilePath(metric, tags_idx):
    path = metric.replace('.', sep)
    return join(settings.LOCAL_DATA_DIR, path, '%d.hs' % tags_idx)


class Archive:
    def __init__(self, secPerPoint, points):
        self.secPerPoint = secPerPoint
        self.points = points

    def __str__(self):
        return 'Archive(%s, %s)' % (secPerPoint, points)

    def getTuple(self):
        return self.secPerPoint, self.points

    @staticmethod
    def fromString(retentionDef):
        rs = kenshin.parse_retention_def(retentionDef)
        return Archive(*rs)


class Schema:
    def match(self, metric):
        raise NotImplementedError()


class DefaultSchema(Schema):
    def __init__(self, name, xFilesFactor, aggregationMethod, archives,
                 cache_retention, tags_num):
        self.name = name
        self.xFilesFactor = xFilesFactor
        self.aggregationMethod = aggregationMethod
        self.archives = archives
        self.cache_retention = cache_retention
        self.tags_num = tags_num

    def match(self, metric):
        return True


class PatternSchema(Schema):
    def __init__(self, name, pattern, xFilesFactor, aggregationMethod, archives,
                 cache_retention, tags_num):
        self.name = name
        self.pattern = re.compile(pattern)
        self.xFilesFactor = xFilesFactor
        self.aggregationMethod = aggregationMethod
        self.archives = archives
        self.cache_retention = cache_retention
        self.tags_num = tags_num

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
        tags_num = options.get('tagsnum')

        try:
            kenshin.validate_archive_list(archives, xff)
        except kenshin.InvalidConfig as e:
            log.err("Invalid schema found in %s." % section)

        schema = PatternSchema(section, pattern, float(xff), agg, archives,
                               int(cache_retention), int(tags_num))
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
)

_schemas = None
def getSchema(metric):
    global _schemas
    if _schemas is None:
        conf_file = join(settings.CONF_DIR, 'storage-schemas.conf')
        _schemas = loadStorageSchemas(conf_file)
    for schema in _schemas:
        if schema.match(metric):
            return schema
    return defaultSchema


if __name__ == '__main__':
    loadStorageSchemas()


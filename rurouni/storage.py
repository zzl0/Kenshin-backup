# coding: utf-8

import os
import re
from os.path import join, sep
from ConfigParser import ConfigParser
from collections import OrderedDict

import kenshin
from rurouni import log
from rurouni.conf import settings, OrderedConfigParser


STORAGE_SCHEMAS_CONFIG = join(settings.CONF_DIR, 'storage-schemas.conf')
STORAGE_AGGREGATION_CONFIG = join(settings.CONF_DIR, 'storage-aggregation.conf')


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
    def __init__(self, name, xFilesFactor, aggregationMethod, archives):
        self.name = name
        self.xFilesFactor = xFilesFactor
        self.aggregationMethod = aggregationMethod
        self.archives = archives

    def match(self, metric):
        return True


class PatternSchema(Schema):
    def __init__(self, name, pattern, xFilesFactor, aggregationMethod, archives):
        self.name = name
        self.pattern = re.compile(pattern)
        self.xFilesFactor = xFilesFactor
        self.aggregationMethod = aggregationMethod
        self.archives = archives

    def match(self, metric):
        return self.pattern.match(metric)


def loadStorageSchemas():
    schema_list = []
    config = OrderedConfigParser()
    config.read(STORAGE_SCHEMAS_CONFIG)

    for section in config.sections():
        options = dict(config.items(section))

        pattern = options.get('pattern')
        xff = float(options.get('xfilesfactor'))
        agg = options.get('aggregationmethod')
        retentions = options.get('retentions').split(',')
        archives = [Archive.fromString(s).getTuple() for s in retentions]

        try:
            kenshin.validate_archive_list(archives, xff)
        except kenshin.InvalidConfig as e:
            log.err("Invalid schema found in %s." % section)

        schema = PatternSchema(section, pattern, float(xff), agg, archives)
        schema_list.append(schema)
    schema_list.append(defaultSchema)
    return schema_list


# default schema

defaultSchema = DefaultSchema(
    'default',
    1.0,
    'avg',
    ((60, 60 * 24 * 7)),  # default retention (7 days of minutely data)
)


if __name__ == '__main__':
    loadStorageSchemas()


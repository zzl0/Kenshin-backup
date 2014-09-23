# coding: utf-8
#
# This module implements various aggregation method.
#


class Agg(object):
    agg_types = [
        'avg',
        'sum',
        'last',
        'max',
        'min',
    ]

    @classmethod
    def get_agg_id(agg_name):
        return cls.agg_types.index(agg_name)

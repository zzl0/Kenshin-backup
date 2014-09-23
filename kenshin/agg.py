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

    @staticmethod
    def get_agg_id(agg_name):
        return Agg.agg_types.index(agg_name)

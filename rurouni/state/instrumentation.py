# coding: utf-8

stats = {}


def incr(stat, amount=1):
    stats.setdefault(stat, 0)
    stats[stat] += amount
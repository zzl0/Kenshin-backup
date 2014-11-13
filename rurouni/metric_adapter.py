# coding: utf-8
import os
import re

from rurouni import log
from rurouni.conf import settings


def load_conf(path):
    if not os.path.exists(path):
        log.err("conf %s do not exists", path)
        raise Exception("conf %s do not exists" % path)
    try:
        data = open(path).read()
        exec data in globals(), globals()
    except Exception, e:
        log.err("error while load conf from %s: %s" % (path, e))
        raise


config_file = os.path.join(settings.CONF_DIR, 'metric_adapter.conf')
load_conf(config_file)


def change_metric(metric, config=METRIC_ADAPTER_CONFIG):
    """
    Change Graphite metric name to Rurouni metric name schema (metric + tags).

    >>> config = [
    ...     ('stats\.counters\.web\.(?P<app>[^.]*)\.bandwidth\.rate',
    ...      'stats.counters.web.bandwidth.rate app={app}')
    ... ]
    >>> change_metric('stats.counters.web.9_douban_com.bandwidth.rate', config)
    stats.counters.web.bandwidth.rate app=9_douban_com
    """
    for (in_pat, out_pat) in config:
        match = re.match(in_pat, metric)
        return out_pat.format(**match.groupdict())


if __name__ == '__main__':
    print change_metric('stats.counters.web.9_douban_com.bandwidth.rate')

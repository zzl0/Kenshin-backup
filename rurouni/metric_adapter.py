# coding: utf-8
import os
import re
import imp

from rurouni.conf import settings

config_file = os.path.join(settings.CONF_DIR, 'metric_adapter.conf')
adapter_conf = imp.load_source('adapter_conf', config_file)


METRIC_RE_EXPS = dict(
    NAME='[^.]+',
    NUM='[0-9]+',
    ANY='.+',
    FLOW_TYPE='bandwidth|errors|qps|reqs'
)
METRIC_ADAPTER_CONFIG = [(re.compile(in_pat.format(**METRIC_RE_EXPS)), out_pat)
                         for (in_pat, out_pat) in adapter_conf.METRIC_ADAPTER_CONFIG]


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
        match = in_pat.match(metric)
        if match:
            return out_pat.format(**match.groupdict()).split(' ')


if __name__ == '__main__':
    print change_metric('stats.counters.web.9_douban_com.bandwidth.rate')

# coding: utf-8
import os
import re
import imp

from rurouni.conf import settings


METRIC_RE_EXPS = dict(
    NAME='[^.]+',
    NUM='[0-9]+',
    ANY='.+',
    FLOW_TYPE='bandwidth|errors|qps|reqs'
)


def loadMetricAdapterConfig(config_file):
    adapter_conf = imp.load_source('adapter_conf', config_file)
    return [(re.compile(in_pat.format(**METRIC_RE_EXPS)), out_pat)
            for (in_pat, out_pat) in adapter_conf.METRIC_ADAPTER_CONFIG]

_config = None
def changeMetric(metric):
    """
    Change Graphite metric name to Rurouni metric name schema (metric + tags).

    >>> config = [
    ...     ('stats\.counters\.web\.(?P<app>[^.]*)\.bandwidth\.rate',
    ...      'stats.counters.web.bandwidth.rate app={app}')
    ... ]
    >>> changeMetric('stats.counters.web.9_douban_com.bandwidth.rate', config)
    stats.counters.web.bandwidth.rate app=9_douban_com
    """
    global _config
    if _config is None:
        config_file = os.path.join(settings.CONF_DIR, 'metric_adapter.conf')
        _config = loadMetricAdapterConfig(config_file)
    for (in_pat, out_pat) in _config:
        match = in_pat.match(metric)
        if match:
            return out_pat.format(**match.groupdict()).split(' ')


if __name__ == '__main__':
    print changeMetric('stats.counters.web.9_douban_com.bandwidth.rate')

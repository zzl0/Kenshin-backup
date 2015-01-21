# utf-8
import os
import ipdb
from collections import defaultdict
from os.path import dirname, abspath

from rurouni.conf import get_parser, settings, read_config


def get_metrics(filename):
    with open(filename) as f:
        for line in f:
            line = line.strip()
            if line:
                yield line


def main():
    bin_dir = dirname(abspath(__file__))
    root_dir = dirname(bin_dir)
    os.environ.setdefault('GRAPHITE_ROOT', root_dir)

    parser = get_parser()
    options, args = parser.parse_args()
    options = vars(options)
    options['logdir'] = None

    cache_settings = read_config('rurouni-cache', options)
    settings.update(cache_settings)

    from rurouni.storage import getSchema, getFilePath, getMetricPath
    filename = '/Users/zzl/projects/bigdata/graphite-root/online_conf/a'
    metric_cnt = defaultdict(int)
    for metric in get_metrics(filename):
        schema = getSchema(metric)
        name, size = schema.name, schema.metrics_max_num
        cnt = metric_cnt.get(name, 0)
        metric_cnt[name] += 1
        print metric, name, cnt / size, cnt % size


if __name__ == '__main__':
    main()
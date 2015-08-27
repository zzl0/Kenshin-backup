#!/usr/bin/env python
# coding: utf-8
import re
import os
import glob


def match_metrics(index_dir, reg_exp):
    reg_exp = re.compile(reg_exp)
    index_files = glob.glob(os.path.join(index_dir, '*.idx'))
    for index in index_files:
        bucket = os.path.splitext(os.path.basename(index))[0]
        with open(index) as f:
            for line in f:
                line = line.strip()
                metric, schema_name, fid, pos = line.split(' ')
                if reg_exp.match(metric):
                    yield ' '.join([bucket, schema_name, fid, pos, metric])


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--dir', required=True, help='directory that contains kenshin index files.')
    parser.add_argument('-r', '--reg-exp', required=True, help='regular expression that matches metrics.')
    args = parser.parse_args()
    for m in match_metrics(args.dir, args.reg_exp):
        print m


if __name__ == '__main__':
    main()

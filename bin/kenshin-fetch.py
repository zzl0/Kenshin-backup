#!/usr/bin/env python
# coding: utf-8

import time
import kenshin


if __name__ == '__main__':
    import sys
    if len(sys.argv) < 3:
        print 'Usage: kenshin-info.py <file_path> <from_ts> [<now_ts>]'
        sys.exit(1)
    path = sys.argv[1]
    from_ts = int(sys.argv[2])
    try:
        now_ts = int(sys.argv[3])
    except:
        now_ts = int(time.time())

    print kenshin.fetch(path, from_ts, now_ts, now_ts)

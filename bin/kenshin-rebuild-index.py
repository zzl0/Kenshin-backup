import os
import sys
import glob
import kenshin
from os.path import basename, splitext


def main():
    if len(sys.argv) < 3:
        print 'need data_dir and index_file'
        sys.exit(1)

    data_dir, index_file = sys.argv[1:]
    out = open(index_file, 'w')

    for schema_name in os.listdir(data_dir):
        hs_file_pat = os.path.join(data_dir, schema_name, '*.hs')
        for fp in glob.glob(hs_file_pat):
            with open(fp) as f:
                header = kenshin.header(f)
                metric_list = header['tag_list'][:-1]
                file_id = splitext(basename(fp))[0]
                for i, metric in enumerate(metric_list):
                    if metric != '':
                        out.write('%s %s %s %s\n' %
                                  (metric, schema_name, file_id, i))
    out.close()


if __name__ == '__main__':
    main()
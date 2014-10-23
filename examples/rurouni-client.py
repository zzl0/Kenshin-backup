# coding: utf-8

import re
import sys
import socket
import time
import subprocess

RUROUNI_SERVER = '127.0.0.1'
RUROUNI_PORT = 2003
DELAY = 60


def get_loadavg():
    cmd = 'uptime'
    output = subprocess.check_output(cmd, shell=True).strip()
    output = re.split("\s+", output)
    return output[-3:]


def run(sock, delay):
    while True:
        now = int(time.time())
        loadavg = get_loadavg()

        lines = []
        idx2min = [1, 5, 15]
        for i, val in enumerate(loadavg):
            line = "system.loadavg_%dmin %s %d" % (idx2min[i], val, now)
            lines.append(line)
        msg = '\n'.join(lines) + '\n'  # all lines must end in a newline
        print 'sending message'
        print '-' * 80
        print msg
        sock.sendall(msg)
        time.sleep(delay)


def main():
    if len(sys.argv) > 1:
        delay = int(sys.argv[1])
    else:
        delay = DELAY

    sock = socket.socket()
    try:
        sock.connect((RUROUNI_SERVER, RUROUNI_PORT))
    except socket.error:
        raise SystemError("Couldn't connect to %s on port %s" %
                          (RUROUNI_SERVER, RUROUNI_PORT))

    try:
        run(sock, delay)
    except KeyboardInterrupt:
        sys.stderr.write("\nexiting on CTRL+c\n")
        sys.exit(0)


if __name__ == '__main__':
    main()

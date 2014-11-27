# coding: utf-8

import sys
import struct
import socket
import cPickle as pickle

RUROUNI_SERVER = '127.0.0.1'
RUROUNI_QUERY_PORT = 7002

def main():
    metric = sys.argv[1]
    conn = socket.socket()
    try:
        conn.connect((RUROUNI_SERVER, RUROUNI_QUERY_PORT))
    except socket.error:
        raise SystemError("Couldn't connect to %s on port %s" %
                          (RUROUNI_SERVER, RUROUNI_QUERY_PORT))

    request = {
        'type': 'cache-query',
        'metric': metric,
    }

    serialized_request = pickle.dumps(request, protocol=-1)
    length = struct.pack('!L', len(serialized_request))
    request_packet = length + serialized_request

    try:
        conn.sendall(request_packet)
        rs = recv_response(conn)
        print rs
    except Exception as e:
        raise e


def recv_response(conn):
    length = recv_exactly(conn, 4)
    body_size = struct.unpack('!L', length)[0]
    body = recv_exactly(conn, body_size)
    return pickle.loads(body)


def recv_exactly(conn, num_bytes):
    buf = ''
    while len(buf) < num_bytes:
        data = conn.recv(num_bytes - len(buf))
        if not data:
            raise Exception("Connection lost.")
        buf += data
    return buf


if __name__ == '__main__':
    main()

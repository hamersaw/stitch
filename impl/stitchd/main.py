#!/bin/python3

import argparse
import socket
import struct
import sys
from threading import Thread

def handle(sock):
    # read image paths 
    sentinel2_count = sock.recv(1, socket.MSG_WAITALL)
    sentinel2_paths = []
    for i in range(0, sentinel2_count[0]):
        path = read_string(sock)
        sentinel2_paths.append(path)

    modis_path = read_string(sock)

    # TODO - process paths
    print(modis_path)

    # TODO - return first sentinel-2 image

    sock.close()

def read_string(sock):
    length_buf = sock.recv(1, socket.MSG_WAITALL)
    length = struct.unpack('>B', length_buf)[0]
    buf = sock.recv(length)
    value = buf.decode('utf-8')
    return value

if __name__ == '__main__':
    # parse arguments
    parser = argparse.ArgumentParser(description='impute stip images')
    parser.add_argument('-i', '--ip-address', type=str,
        help='server ip address', default='0.0.0.0')
    parser.add_argument('-p', '--port', type=int,
        help='server port', default='12289')

    args = parser.parse_args()

    # open server socket
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        server_sock.bind((args.ip_address, args.port))
    except socket.error as msg:
        print('failed to bind socket: ' + str(msg[0]) + ' ' + msg[1])
        sys.exit()

    # listen for client connections
    server_sock.listen()
    while 1:
        # accept connection
        sock, address = server_sock.accept()

        # start new thread to handle connection
        try:
            Thread(target=handle, args=(sock, )).start()
        except:
            print('failed to start thread')

    # close server socket
    server_sock.close()

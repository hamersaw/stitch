#!/bin/python3

import argparse
import socket
import sys
from threading import Thread

def handle(connection):
    print('TODO - handling connection')

if __name__ == '__main__':
    # parse arguments
    parser = argparse.ArgumentParser(description='impute stip images')
    parser.add_argument('-i', '--ip-address', type=str,
        help='server ip address', default='127.0.0.1')
    parser.add_argument('-p', '--port', type=int,
        help='server port', default='12289')

    args = parser.parse_args()

    # open server socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind((args.ip_address, args.port))
    except socket.error as msg:
        print('failed to bind socket: ' + str(msg[0]) + ' ' + msg[1])
        sys.exit()

    # listen for client connections
    sock.listen()
    while 1:
        # accept connection
        connection, address = sock.accept()

        # start new thread to handle connection
        try:
            Thread(target=handle, args=(connection)).start()
        except:
            print('failed to start thread')

    # close server socket
    sock.close()

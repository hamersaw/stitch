#!/bin/python3

import argparse
import multiprocessing as mp
import numpy as np
from sklearn.preprocessing import LabelEncoder
import socket
import sys
import time
import tensorflow as tf
import traceback

import impute
import serialize

# disable eager mode -> run in graph mode
tf.compat.v1.disable_eager_execution()

def worker(geohashes, model_path, server_sock, weight_path):
    # initialize encoder
    le = LabelEncoder()
    encoder = le.fit(args.geohash)

    # open model
    tf.keras.backend.clear_session()

    layers = open(args.model, 'r')
    model_structure = layers.read()
    layers.close()

    model = tf.keras.models.model_from_json(model_structure)
    model.load_weights(args.weights)

    # first prediction is time consuming, building the GPU function
    model.predict((np.zeros((1, 3, 256, 256, 3)),
                        np.zeros((1, 1)),
                        np.zeros((1, 1)),
                        np.zeros((1, 1)),
                        np.zeros((1, 16, 16, 3))))

    # make model read only, thread safe
    session = tf.compat.v1.keras.backend.get_session()
    tf.python.keras.backend.set_session(session)
    session.graph.finalize()

    while 1:
        try:
            # accept connection
            sock, address = server_sock.accept()

            # read batch metadata
            sentinel2_batch, modis_batch, geohash_batch, \
                timestamp_batch = serialize.read_batch(sock)

            # compute input tensor
            tensor = impute.compile_tensor(sentinel2_batch,
                modis_batch, encoder, geohash_batch, timestamp_batch)

            # impute images
            imputed_images = impute.impute_batch(model, tensor)

            # write imputed images
            serialize.write_images(imputed_images,
                sentinel2_batch[0][0], sock)

            # close client connection
            sock.close()
        except:
            traceback.print_exc()

if __name__ == '__main__':
    # parse arguments
    parser = argparse.ArgumentParser(description='impute stip images')
    parser.add_argument('-i', '--ip-address', type=str,
        help='server ip address', default='0.0.0.0')
    parser.add_argument('-g', '--geohash', action='append',
        help='geohashes handled by this node', required=True)
    parser.add_argument('-m', '--model',
        help='model location', required=True)
    parser.add_argument('-p', '--port', type=int,
        help='server port', default='12289')
    parser.add_argument('-t', '--thread-count', type=int,
        help='number of worker threads', default=33)
    parser.add_argument('-w', '--weights',
        help='model weights location', required=True)

    args = parser.parse_args()

    # open server socket
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        server_sock.bind((args.ip_address, args.port))
        server_sock.listen(8)
    except socket.error as msg:
        print('failed to bind socket: ' + str(msg[0]) + ' ' + msg[1])
        sys.exit()

    # initialize workers
    workers = [mp.Process(target=worker, args=(args.geohash, args.model, server_sock, args.weights, )) for i in range(args.thread_count)]

    # start workers
    for p in workers:
        p.daemon = True
        p.start()

    while True:
        try:
            time.sleep(10)
        except KeyboardInterrupt:
            if server_sock:
                server_sock.close()
            break
        except:
            traceback.print_exc()
            break

    if server_sock:
        server_sock.close()

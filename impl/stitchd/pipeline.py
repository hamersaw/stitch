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

def io_worker(geohashes, pipe, server_sock):
    # initialize encoder
    le = LabelEncoder()
    encoder = le.fit(args.geohash)

    while 1:
        try:
            # accept connection
            sock, address = server_sock.accept()

            # read batch metadata
            read_start = time.time()
            sentinel2_batch, modis_batch, geohash_batch, \
                timestamp_batch = serialize.read_batch(sock)
            read_duration = time.time() - read_start

            if len(sentinel2_batch) > 1:
                raise Exception('batch_size > 1 not supported')

            # compute input tensor
            compile_start = time.time()
            tensor = impute.compile_tensor(sentinel2_batch,
                modis_batch, encoder, geohash_batch, timestamp_batch)
            compile_duration = time.time - compile_start

            # impute images
            impute_start = time.time()
            pipe.send(tensor)
            imputed_images = pipe.recv()
            impute_duration = time.time() - impute_start

            # write imputed images
            write_start = time.time()
            serialize.write_images(imputed_images,
                sentinel2_batch[0][0], sock)
            write_duration = time.time() - write_start

            print(str(read_duration) + ' ' + str(compile_duration) + ' '
                + str(impute_duration) + ' ' + str(write_duration))

            # close client connection
            sock.close()
        except:
            traceback.print_exc()

def impute_worker(model_path, pipes, weights_path):
    # open model
    tf.keras.backend.clear_session()

    layers = open(model_path, 'r')
    model_structure = layers.read()
    layers.close()

    model = tf.keras.models.model_from_json(model_structure)
    model.load_weights(weights_path)

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

    index = 0
    indices = []
    tensor = [[], [], [], [], []]
    while 1:
        try:
            # read tensors
            count = 0
            while count != len(pipes) and len(indices) != 10:
                if pipes[index].poll():
                    pipe_tensor = pipes[index].recv()

                    # append data
                    indices.append(index)
                    for j in range(len(tensor)):
                        tensor[j].append(pipe_tensor[j][0])

                index = (index + 1) % len(pipes)
                count += 1

            # if no data -> sleep, continue
            if len(indices) == 0:
                time.sleep(0.1)
                continue

            # impute images
            imputed_images = impute.impute_batch(model, tensor)

            # write imputed images to pipe
            for i in range(len(imputed_images)):
                pipes[indices[i]].send([imputed_images[i]])

            # clear indices and tensor
            indices.clear()
            for element in tensor:
                element.clear()
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

    # initialize pipes
    io_pipes = []
    impute_pipes = []
    for i in range(args.thread_count):
        a, b = mp.Pipe(True)

        io_pipes.append(a)
        impute_pipes.append(b)

    # initialize workers
    impute_worker = mp.Process(target=impute_worker, args=(args.model, impute_pipes, args.weights, ))

    io_workers = []
    for i in range(args.thread_count):
        worker = mp.Process(target=io_worker, args=(args.geohash, io_pipes[i], server_sock, ))

        io_workers.append(worker)

    # start workers
    impute_worker.daemon = True
    impute_worker.start()

    for worker in io_workers:
        worker.daemon = True
        worker.start()

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

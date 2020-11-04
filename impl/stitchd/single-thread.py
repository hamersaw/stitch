#!/bin/python3

import argparse
import cv2
import gdal
import numpy as np
from sklearn.preprocessing import LabelEncoder
import socket
import struct
import sys
import tensorflow as tf

import os
script_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(script_dir + '/../../../stippy/')
import ImputationGeneration

# disable eager mode -> run in graph mode
tf.compat.v1.disable_eager_execution()

def read_string(sock):
    length_buf = sock.recv(1, socket.MSG_WAITALL)
    length = struct.unpack('>B', length_buf)[0]
    buf = sock.recv(length)
    value = buf.decode('utf-8')
    return value

def write_string(string, sock):
    buf = str.encode(string)
    sock.sendall(struct.pack('>I', len(buf)))
    sock.sendall(buf)

def impute_image(sentinel2_paths, modis_path,
        model, encoder, geohashes, timestamps):
    sentinel2_imgs = ImputationGeneration \
        .sentinel2_path_to_image_helper(sentinel2_paths)
    modis_imgs = np.array(ImputationGeneration.paths_to_rgb_convertor(
        modis_path, isSentinel=False))

    target_geo, target_woy, target_soy = ImputationGeneration \
        .preprocess_inputs(encoder, timestamps, geohashes)

    imputed_image = ImputationGeneration.unscale_images(
        model.predict([sentinel2_imgs, target_geo, target_woy,
                target_soy, modis_imgs]
            )).astype(np.uint8)

    return imputed_image

#def handle(encoder, session, model_graph, model, sock):
def handle(encoder, model, sock):
    # read batch size
    batch_size = sock.recv(1, socket.MSG_WAITALL)[0]

    sentinel2_batch = []
    modis_batch = []
    geohash_batch = []
    timestamp_batch = []
    for i in range(0, batch_size):
        # read geohash and timestamp
        geohash = read_string(sock)
        #geohash_batch.append(geohash)
        geohash_batch.append('9q6qp') # TODO - necessary for testing

        timestamp_buf = sock.recv(8, socket.MSG_WAITALL)
        timestamp = struct.unpack('>q', timestamp_buf)[0]
        timestamp_batch.append(timestamp)

        # read image paths 
        sentinel2_count = sock.recv(1, socket.MSG_WAITALL)[0]
        sentinel2_paths = []
        for i in range(0, sentinel2_count):
            path = read_string(sock)
            sentinel2_paths.append(path)
        sentinel2_batch.append(sentinel2_paths);

        modis_path = read_string(sock)
        modis_batch.append(modis_path)

    # impute images
    imputed_images = impute_image(sentinel2_batch, modis_batch,
        model, encoder, geohash_batch, timestamp_batch)

    # open datset
    dataset = gdal.Open(sentinel2_batch[0][0])

    # write success
    sock.sendall(struct.pack('B', 0))

    for i in range(0, batch_size):
        # write image dimensions
        sock.sendall(struct.pack('>I', dataset.RasterXSize))
        sock.sendall(struct.pack('>I', dataset.RasterYSize))

        # write geotransform
        for value in dataset.GetGeoTransform():
            sock.sendall(struct.pack('>d', value))

        # write projection
        projection = dataset.GetProjection()
        write_string(projection, sock)

        # write gdal_type and no_data_value
        band = dataset.GetRasterBand(1)

        sock.sendall(struct.pack('>I', band.DataType))

        no_data_value = band.GetNoDataValue()
        if no_data_value != None:
            sock.sendall(struct.pack('>B', 1))
            sock.sendall(struct.pack('>d', no_data_value))
        else:
            sock.sendall(struct.pack('>B', 0))

        # resize the image
        imputed_image = imputed_images[i]
        imputed_image = cv2.resize(imputed_image, 
            dsize=(dataset.RasterXSize, dataset.RasterYSize),
            interpolation=cv2.INTER_CUBIC)

        # write rasters
        sock.sendall(struct.pack('>B', dataset.RasterCount))
        for i in range(0, dataset.RasterCount):
            band = dataset.GetRasterBand(i+1)

            # write band type
            data_type = band.DataType
            sock.sendall(struct.pack('>I', band.DataType))

            if data_type != gdal.GDT_Byte:
                # TODO - throw error
                print('unsupported data type')
                continue

            for j in range(0, band.YSize):
                for k in range(0, band.XSize):
                    sock.sendall(imputed_image[j][k][i])

            # read data
            #data = band.ReadRaster(xoff=0, yoff=0,
            #    xsize=band.XSize, ysize=band.YSize,
            #    buf_xsize=band.XSize, buf_ysize=band.YSize,
            #    buf_type=data_type)

            # write data
            #if data_type == gdal.GDT_Byte:
            #    sock.sendall(data)
            #elif data_type == gdal.GDT_Int16:
            #    for value in data:
            #        sock.sendall(struct.pack('>h', value))
            #elif data_type == gdal.GDT_UInt16:
            #    for value in data:
            #        sock.sendall(struct.pack('>H', value))
            #else:
            #    # TODO - throw error
            #    print('unsupported data type')

    sock.close()

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
    parser.add_argument('-w', '--weights',
        help='model weights location', required=True)

    args = parser.parse_args()

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

    # First prediction is time consuming, building the GPU function
    model.predict((np.zeros((1, 3, 256, 256, 3)),
                        np.zeros((1, 1)),
                        np.zeros((1, 1)),
                        np.zeros((1, 1)),
                        np.zeros((1, 16, 16, 3))))

    session = tf.compat.v1.keras.backend.get_session()

    # make model read only, thread safe
    tf.python.keras.backend.set_session(session)
    session.graph.finalize()

    # open server socket
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        server_sock.bind((args.ip_address, args.port))
    except socket.error as msg:
        print('failed to bind socket: ' + str(msg[0]) + ' ' + msg[1])
        sys.exit()

    # listen for client connections
    try:
        server_sock.listen()
        while 1:
            # accept connection
            sock, address = server_sock.accept()

            # start new thread to handle connection
            handle(encoder, model, sock)

            # close remote connection
            sock.close()
    except KeyboardInterrupt:
        if server_sock:
            server_sock.close()
    except:
        traceback.print_exc()

    if server_sock:
        server_sock.close()

#!/bin/python3

import cv2
import gdal
import socket
import struct

def read_batch(sock):
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

    return sentinel2_batch, modis_batch, geohash_batch, timestamp_batch

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

def write_images(imputed_images, sentinel2_path, sock):
    # open datset
    dataset = gdal.Open(sentinel2_path)

    # write success
    sock.sendall(struct.pack('B', 0))

    #for i in range(0, batch_size):
    for imputed_image in imputed_images:
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

        # resize the imputed image
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

            data = []
            for j in range(0, band.YSize):
                for k in range(0, band.XSize):
                    data.append(imputed_image[j][k][i])

            sock.sendall(bytes(data))

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

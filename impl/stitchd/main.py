#!/bin/python3

import argparse
import gdal
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

    # TODO - impute image
    #print(modis_path)

    # write success
    sock.sendall(struct.pack('B', 0))

    # open datset
    dataset = gdal.Open(sentinel2_paths[0])

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

    # write rasters
    sock.sendall(struct.pack('>B', dataset.RasterCount))
    for i in range(0, dataset.RasterCount):
        band = dataset.GetRasterBand(i+1)

        # write band type
        data_type = band.DataType
        sock.sendall(struct.pack('>I', band.DataType))

        # read data
        data = band.ReadRaster(xoff=0, yoff=0,
            xsize=band.XSize, ysize=band.YSize,
            buf_xsize=band.XSize, buf_ysize=band.YSize,
            buf_type=data_type)

        # write data
        if data_type == gdal.GDT_Byte:
            sock.sendall(data)
        elif data_type == gdal.GDT_Int16:
            for value in data:
                sock.sendall(struct.pack('>h', value))
        elif data_type == gdal.GDT_UInt16:
            for value in data:
                sock.sendall(struct.pack('>H', value))
        else:
            # TODO - throw error
            print('unsupported data type')

    sock.close()

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

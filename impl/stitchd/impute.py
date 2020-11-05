#!/bin/python3

import numpy as np
from sklearn.preprocessing import LabelEncoder
import sys
import os

script_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(script_dir + '/../../../stippy/')

import ImputationGeneration


def compile_tensor(sentinel2_paths, modis_path,
        encoder, geohashes, timestamps):
    # read sentinel2 and modis images from path
    sentinel2_imgs = ImputationGeneration \
        .sentinel2_path_to_image_helper(sentinel2_paths)
    modis_imgs = np.array(ImputationGeneration.paths_to_rgb_convertor(
        modis_path, isSentinel=False))

    # preprocess inputs
    target_geo, target_woy, target_soy = ImputationGeneration \
        .preprocess_inputs(encoder, timestamps, geohashes)

    # return aggregate tensor
    return [sentinel2_imgs, target_geo, target_woy, 
        target_soy, modis_imgs]

def impute_batch(model, tensor):
    imputed_image = ImputationGeneration.unscale_images(
        model.predict(tensor)).astype(np.uint8)

    return imputed_image

# A standard multiprocessing library mated to zarr - a standard example of writing to zarr file using multiprocessing.
# View discussion and source code here: https://github.com/zarr-developers/zarr-python/issues/199
# View block / hang discussion here: https://github.com/zarr-developers/numcodecs/issues/41
# View using Dask for multi-threading here: https://clouds.eos.ubc.ca/~phil/courses/parallel_python/03_dask_and_zarr.html

import os
import dask
import dask.array as da
from dask_image.imread import imread
import glob
import zarr
import numpy as np
from pprint import pprint
import multiprocessing
from zarr import blosc
blosc.set_nthreads(20)
blosc.use_threads=False  # This must be set to false to prevent locking in the blosc context
GPU = 'C:\\Users\\Viz2\\python_anaconda3\\UCB ABC\\demo_napari\\sandbox\\GPU\\raw_tiff'

"""Let's first take a look at processing tiff images for zarr exporting"""
def get_filenames(directory):
    filenames = glob.glob(f"{directory}\\*.tif")
    return filenames

def imread_tiff(directory, num_folder):
    if not isinstance(directory, str) or not isinstance(num_folder, int):
        raise ValueError('check input types!')
    tiff_files = [directory + '\\' + file for file in os.listdir(directory)]
    tiff_files_trimmed = tiff_files[num_folder:]
    sample = imread(tiff_files_trimmed[0])
    print(sample.shape)
    return sample

def get_lazy_arrays(glob_filenames, imread_sample):
    lazy_arrays = [dask.delayed(imread)(fn) for fn in glob_filenames]
    lazy_arrays = [da.from_delayed(x, shape=imread_sample.shape, dtype=imread_sample.dtype)
              for x in lazy_arrays]
    return lazy_arrays

def parse_regex_tiff(glob_filenames, lazy_arrays):
    # Get various dimensions
    # THIS IS FOR PARSING BY SCAN_ITER AND CHANNELS
    # e.g. Scan_Iter_0000_CamA_ch0_CAM1_stack0000_488nm_0000000msec_0016966725msecAbs_000x_000y_000z_0000t.tif

    glob_filenames_terminal = [file.split('\\')[-1] for file in glob_filenames]
    fn_comp_sets = dict()
    for fn in glob_filenames_terminal:
        for i, comp in enumerate(os.path.splitext(fn)[0].split("_")):
            fn_comp_sets.setdefault(i, set())
            fn_comp_sets[i].add(comp)
    fn_comp_sets = list(map(sorted, fn_comp_sets.values()))

    remap_comps = [
        dict(map(reversed, enumerate(fn_comp_sets[2]))),  # MUST be the index for scan_iter, e.g. '0003'
        dict(map(reversed, enumerate(fn_comp_sets[4])))  # MUST be the index for channel, e.g. 'ch0'
    ]

    # Create an empty object array to organize each chunk that loads a TIFF
    b = np.empty(tuple(map(len, remap_comps)) + (1, 1, 1), dtype=object)

    for fn, x in zip(glob_filenames_terminal, lazy_arrays):
        scan_iter = int(fn[fn.index("Scan_Iter_") + 10:fn.index("_Cam")].split("_")[0])
        channel = int(fn[fn.index("_ch") + 3:].split("_")[0])
        print(scan_iter, channel)

        b[scan_iter, channel, 0, 0, 0] = x
    # YOU MUST HAVE SIMILAR CHANNEL TO SCAN_ITER PATTERNS OR ELSE THE PROCESS WILL FAIL
    # e.g. every Scan_Iter_ must have 8x ch0 and 4x ch1. Deviate from this pattern will result in an exception!

    # Stitch together the many blocks into a single array
    b = da.block(b.tolist())
    return b

# def prep_zarr(sync_name, zarr_name, dask_array):
#     synchronizer = zarr.ProcessSynchronizer(f'{sync_name}.sync')
#     processed_zarr = zarr.hierarchy.open_group(f"{zarr_name}.zarr", 'a', synchronizer=synchronizer)
#
#     features_arr = dask_array
#     shape=features_arr.shape
#     print(shape)
#     processed_zarr.create_dataset("dask_array", data=features_arr, shape=features_arr.shape, dtype="float64", overwrite=True)
#     return processed_zarr
#
# def zarr_write(zarr_dataset):
#     x = zarr_dataset["dask_array"]
#     print(x)



if __name__ == '__main__':
    from dask.distributed import Client, progress
    from numcodecs import Blosc

    filenames = get_filenames(GPU)
    sample = imread_tiff(GPU, 2)
    lazy_arrays = get_lazy_arrays(filenames, sample)
    # print(sample, lazy_arrays, filenames)
    b = parse_regex_tiff(filenames, lazy_arrays)
    print(b.chunks)
    client = Client()
    print(client)
    out = b.to_zarr("test_mydata.zarr", compressor=Blosc(cname='zstd', clevel=3, shuffle=Blosc.BITSHUFFLE))
    try:
        from dask.distributed import Client, progress
        progress(client)
        fut = client.compute(out)
    except BrokenPipeError:
        print('Process complete (likely)...')
    # Now try to run multiprocessing in exporting to zarr file


# # look into synchronized zarr API: https://zarr.readthedocs.io/en/stable/api/sync.html
# synchronizer = zarr.ProcessSynchronizer('example.sync')
# processed_zarr = zarr.hierarchy.open_group("test.zarr", 'a', synchronizer=synchronizer)
#
# features_arr = np.random.random_sample((10000,20))
# processed_zarr.create_dataset("features_arr", data=features_arr, shape=features_arr.shape, dtype="float64", overwrite=True)
#
# ixs = np.arange(processed_zarr["features_arr"].shape[0])
# slices = np.linspace(0, processed_zarr["features_arr"].shape[0]-1, 100, dtype=np.int32)
#
# sliceIter = []
# for i in range(len(slices)-1):
#     sliceIter.append({
#         "min" : ixs[slices[i]],
#         "max" : ixs[slices[i+1]],
#         "slice_num" : i,
#     })
# pprint(sliceIter)
#
# ### slices breakds up the np.arange of processed_zarr["features_arr"] into n number of slices in an np.linspace
# #
# def mem_instantiate(param_dict):
#     min_ix = param_dict["min"]
#     max_ix = param_dict["max"]
#     slice_num = param_dict["slice_num"]
#
#     ### never gets past loading the features
#     instantiated_features = processed_zarr["features_arr"][min_ix:max_ix]
#     print(slice_num, "features loaded")
#
#
# if __name__ == '__main__':
#     pool = multiprocessing.Pool(processes=5)
#     pool.map(mem_instantiate, sliceIter)
#     pool.close()
#     pool.join()

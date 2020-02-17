import os
import glob
import dask
import dask.array as da
from dask.distributed import Client, progress
from dask_image.imread import imread
from numcodecs import Blosc
import numpy as np
import zarr
GPU = 'C:\\Users\\Viz2\\python_anaconda3\\UCB ABC\\demo_napari\\sandbox\\GPU\\raw_tiff'
"""
A module to read and compress (i.e. write) LLS .tif images to zarr files.
This module is for a Windows system.
"""


def get_filenames(directory: str) -> list:
    """Find all pathnames matching the *.tif pattern
    according to the rules used by the Unix shell"""
    filenames = glob.glob(f'{directory}\\*.tif')
    return filenames


def imread_tiff(directory: str, num_folder: int) -> dask.array:
    """Navigate to directory with .tif files and return
    images in a dask array."""
    dir_files = [directory + '\\' + file for file in os.listdir(directory)]
    tif_files = dir_files[num_folder:]
    sample = imread(tif_files[0])
    return sample


def get_lazy_arrays(tif_filepath: list, imread_sample: dask.array) -> list:
    """Lazily load and stich images together with dask array."""
    lazy_read = [dask.delayed(imread)(fn) for fn in tif_filepath]
    lazy_arrays = [da.from_delayed(x, shape=imread_sample.shape, dtype=imread_sample.dtype)
              for x in lazy_read]
    return lazy_arrays


def block_regex_tif(tif_filepath: str, lazy_arrays: list) -> dask.array:
    """Sort .tif files in order. Map key regex components to set chunking
    for .tif array. Block these chunks together and return as dask.array"""
    # THIS IS FOR PARSING BY SCAN_ITER AND CHANNELS
    # e.g. Scan_Iter_0000_CamA_ch0_CAM1_stack0000_488nm_0000000msec_0016966725msecAbs_000x_000y_000z_0000t.tif
    tif_files = [fn.split('\\')[-1] for fn in tif_filepath]
    fn_comp_sets = dict()
    for fn in tif_files:
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
    for fn, x in zip(tif_files, lazy_arrays):
        scan_iter = int(fn[fn.index("Scan_Iter_") + 10:fn.index("_Cam")].split("_")[0])
        channel = int(fn[fn.index("_ch") + 3:].split("_")[0])
        b[scan_iter, channel, 0, 0, 0] = x

    # YOU MUST HAVE SIMILAR CHANNEL PATTERNS TO SCAN_ITER PATTERNS OR ELSE THE PROCESS WILL FAIL
    # e.g. every Scan_Iter_ must have 8x ch0 and 4x ch1. Deviate from this pattern will result in an exception!
    # Stitch together the many blocks into a single array
    b = da.block(b.tolist())
    return b


def parallel_write(filename: str, darray: dask.array) -> None:
    """Distribute Zarr writing task to workers using dask.
    Input filename should have extension .zarr"""
    client = Client()
    out = darray.to_zarr(filename, compressor=Blosc(cname='zstd', clevel=3, shuffle=Blosc.BITSHUFFLE),
        compute=False)
    try:
        progress(client) #  I believe this is for visualization purpose.
        fut = client.compute(out)
    except BrokenPipeError:
        print('Process complete (likely)...')


if __name__ == '__main__':
    filenames = get_filenames(GPU)
    sample = imread_tiff(GPU, num_folder=2)
    lazy_arrays = get_lazy_arrays(filenames, sample)
    b = block_regex_tif(filenames, lazy_arrays)
    parallel_write('test_mydata.zarr', b)

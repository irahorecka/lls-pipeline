import os
import dask.array as da
import zarr
"""
A module to read LLS .tif images from a zarr format.
This module is for a Windows system.
"""

def to_dask_array(zarr_filepath: str) -> da:
    """Read zarr file to dask.array"""
    d = da.from_zarr(zarr_filepath)
    return d


def to_intermediate(zarr_filepath: str) -> zarr.array:
    """Read zarr files to zarr intermediate"""
    z = zarr.open_array(zarr_filepath)
    return z


def retrieve_zarr(zarr_filepath: str, data_type: str='dask'):
    if data_type == 'dask':
        return to_dask_array(zarr_filepath)
    elif data_type == 'zarr':
        return to_intermediate(zarr_filepath)
    else:
        raise ValueError("Please put in a valid data_type kwarg.")

if __name__ == '__main__':
    zarr_filepath = r'C:\Users\Viz2\python_anaconda3\UCB ABC\git_clones\LLS_Pipeline\Exercises\Zarr\test_mydata.zarr'
    d = to_dask_array(zarr_filepath)
    print(d)

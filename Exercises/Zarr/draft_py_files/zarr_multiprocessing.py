# A standard multiprocessing library mated to zarr - a standard example of writing to zarr file using multiprocessing.
# View discussion and source code here: https://github.com/zarr-developers/zarr-python/issues/199
# View block / hang discussion here: https://github.com/zarr-developers/numcodecs/issues/41
# View using Dask for multi-threading here: https://clouds.eos.ubc.ca/~phil/courses/parallel_python/03_dask_and_zarr.html

import zarr
import numpy as np
from pprint import pprint
import multiprocessing
from zarr import blosc
blosc.set_nthreads(20)
blosc.use_threads=False  # This must be set to false to prevent locking in the blosc context 

# look into synchronized zarr API: https://zarr.readthedocs.io/en/stable/api/sync.html
synchronizer = zarr.ProcessSynchronizer('example.sync')
processed_zarr = zarr.hierarchy.open_group("test.zarr", 'a', synchronizer=synchronizer)

features_arr = np.random.random_sample((10000,20))
processed_zarr.create_dataset("features_arr", data=features_arr, shape=features_arr.shape, dtype="float64", overwrite=True)

ixs = np.arange(processed_zarr["features_arr"].shape[0])
slices = np.linspace(0, processed_zarr["features_arr"].shape[0]-1, 100, dtype=np.int32)

sliceIter = []
for i in range(len(slices)-1):
    sliceIter.append({
        "min" : ixs[slices[i]],
        "max" : ixs[slices[i+1]],
        "slice_num" : i,
    })
pprint(sliceIter)

### slices breakds up the np.arange of processed_zarr["features_arr"] into n number of slices in an np.linspace
#
def mem_instantiate(param_dict):
    min_ix = param_dict["min"]
    max_ix = param_dict["max"]
    slice_num = param_dict["slice_num"]

    ### never gets past loading the features
    instantiated_features = processed_zarr["features_arr"][min_ix:max_ix]
    print(slice_num, "features loaded")


if __name__ == '__main__':
    pool = multiprocessing.Pool(processes=5)
    pool.map(mem_instantiate, sliceIter)
    pool.close()
    pool.join()
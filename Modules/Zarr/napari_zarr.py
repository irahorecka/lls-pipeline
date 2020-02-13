import dask.array as da
from dask_image.imread import imread
import napari
import read_zarr


if __name__ == '__main__':
    zarr_filepath = r'C:\Users\Viz2\python_anaconda3\UCB ABC\git_clones\LLS_Pipeline\Exercises\Zarr\test_mydata.zarr'
    d = read_zarr.retrieve_zarr(zarr_filepath, 'dask')
    channel = d.shape.index(min(d.shape))
    with napari.gui_qt():
        blobs = da.stack(d)
        print(blobs.shape, channel)
        # viewer = napari.view_image(blobs.astype(float))
        napari.view_image(blobs, channel_axis=channel)  # d as the first arg will also work.

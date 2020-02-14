import dask.array as da
from dask_image.imread import imread
import napari
import read_zarr
import time
"""An example of viewing and manipulating image files in napari
in real time (i.e. the 4D aspect will work as you have the viewer open).
This is a bit laggy with large files, but perhaps for smaller .tif files
this will be OK."""


if __name__ == '__main__':
    """Currently able to view one channel with the option of
    switching back and forth. See if you can overlay channels"""
    zarr_filepath = r'C:\Users\Viz2\python_anaconda3\UCB ABC\git_clones\LLS_Pipeline\Exercises\Zarr\test_mydata.zarr'
    d = read_zarr.retrieve_zarr(zarr_filepath, 'dask')
    channel = d.shape.index(min(d.shape))
    with napari.gui_qt():
        # create the viewer with an image
        data = [i for i in da.stack(d)]
        viewer = napari.Viewer(ndisplay=3)
        layer = viewer.add_image(data[0])

        def layer_update(*, update_period):

            # number of times to update
            for k in data:
                time.sleep(update_period)

                dat = k
                layer.data = dat

                # check that data layer is properly assigned and not blocked?
                while layer.data.all() != dat.all():
                    layer.data = dat

        viewer.update(layer_update, update_period=0.05)

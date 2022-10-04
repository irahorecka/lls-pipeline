# lls-pipeline
## Introduction
A post-processing pipeline for lattice light-sheet microscopy images.

## Libraries of interest
- ```dask```
- ```napari```
- ```naparimovie```
- ```pycudadeconv```
- ```pyopencl```
- ```zarr```

## Example workflow
```tif``` --> ```zarr``` --> ```dask.array``` --> [post-processing] --> ```napari``` --> ```naparimovie```

## Example movie
Below is a movie generated via this pipeline. The pipeline is in its very early stages.

![unknown LLS](https://i.imgur.com/kc5FOjg.gif)

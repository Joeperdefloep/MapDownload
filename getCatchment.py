#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 21 19:44:49 2020

@author: joempie
"""
import os
from pysheds.grid import Grid
import richdem as rd

filledDemPath = '../data/N09E037.tif'
x,y = 37.15251,9.55677


if (not os.path.isfile(filledDemPath)):
    dem = rd.LoadGDAL('../data/N09E037.hgt')
    rd.FillDepressions(dem,epsilon=True,in_place=True)
    rd.SaveGDAL(filledDemPath,dem)

grid = Grid.from_raster(filledDemPath, data_name='infl_dem')
# grid.fill_depressions(data='dem',out_name='flooded_dem')
# grid.resolve_flats(data='flooded_dem')


# Specify directional mapping
dirmap = (64, 128, 1, 2, 4, 8, 16, 32)
grid.flowdir(data='infl_dem', out_name='dir', dirmap=dirmap)

# Delineate the catchment
grid.catchment(data='dir', x=x, y=y, dirmap=dirmap, out_name='catch',
               recursionlimit=15000, xytype='label')
# Crop and plot the catchment
# ---------------------------
# Clip the bounding box to the catchment
grid.clip_to('catch')
grid.view('catch')
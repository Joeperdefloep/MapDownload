#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 26 10:29:03 2020

@author: joempie
"""
import os
import geopandas as gpd
from owslib.wcs import WebCoverageService
from multiprocessing.pool import ThreadPool
import rasterio as rio

#define constants for the ISRIC soil data

#bounds for our specific site



#%%

class SoilDownload:
    WCRS = 'PROJCS["Interrupted_Goode_Homolosine",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0],UNIT["Degree",0.0174532925199433]],PROJECTION["Interrupted_Goode_Homolosine"],UNIT["metre",1,AUTHORITY["EPSG","9001"]],AXIS["Easting",EAST],AXIS["Northing",NORTH]]'
    DEPTHS = ['0-5','5-15','15-30','30-60','60-100','100-200']
    DATATYPES=['Q0.5','Q0.05','Q0.95','mean','uncertainty']
    MAPTYPES=['bdod','cec','cfvo','clay','nitrogen','phh2o','sand','silt','soc','ocd']#,'gyga','af250m_nutrient','wosis_latest','external']
    OTHERMAPS=['gyga','af250m_nutrient','wosis_latest','external']
    
    def __init__(self, like=None, download=False):
        if type(like) == str:
            try:
                likeFile = gpd.read_file(like)
                self.bounds = likeFile.to_crs(WCRS).bounds
            except:
                likeFile = rio.open(like)
                
                
            
    
    
    
    def getCube(self, maptype, depths, datatypes, bounds, overwrite=False):
        #check if all files are already downloaded
        fpaths = ['../tmp/Hadocha_'+maptype+'_'+depth+'cm_'+datatype+'.tif' for depth in depths for datatype in datatypes]
        if (min([os.path.isfile(fpath) for fpath in fpaths]) and not overwrite):
            print('all files already downloaded for: '+maptype)
            return fpaths
        
        #set up the Web Coverage Service. This sometimes raises errors, so retry
        print('setting up WCS for maptype: '+maptype)
        for attempt in range(5):
            try:    
                wcs = WebCoverageService('http://maps.isric.org/mapserv?map=/map/'+maptype+'.map',
                                         version='1.0.0')        
            except:
                print('failed to connect to WCS '+str(attempt+1)+' times to: '+maptype)
            else:
                break
        else:
            print('failed to connect to WCS, try again later')
            return []
    
        #start retrieving map data
        for depth in depths:
            for datatype in datatypes:
                fpath = '../tmp/Hadocha_'+maptype+'_'+depth+'cm_'+datatype+'.tif'
                #also handle exceptions for retrieving files
                if(not os.path.isfile(fpath) or overwrite):
                    for attempt in range(5):
                        try:
                            response = wcs.getCoverage(
                                identifier=maptype+'_'+depth+'cm_'+datatype, 
                                crs='urn:ogc:def:crs:EPSG::152160',
                                bbox=(bounds.minx,bounds.miny,bounds.maxx,bounds.maxy), 
                                resx=20, resy=20, 
                                format='GEOTIFF_INT16')
                        except:
                            print('coverage failed '+str(attempt+1)+' times for: '+fpath)
                        else:
                            with open(fpath, 'wb') as file:
                                file.write(response.read())
                                # print('file: '+fpath+'downloaded')
                            break
                    else:
                        print('file not retrieved: '+fpath)
                        fpaths.remove(fpath)
                    # else:
                    #     print('file already downloaded: '+fpath)
        # print('map at location: '+fpath)
        return fpaths

    def getCubes(self, 
                 maptypes=None,
                 depths=None,
                 datatypes=None,
                 bounds=None,
                 overwrite=False):
        
        if maptypes=='all':
            maptypes = self.MAPTYPES
        if depths=='all':
            depths=self.DEPTHS
        if datatypes=='all':
            datatypes=self.DATATYPES
        
        if type(maptypes) is str: maptypes = [maptypes]
        if type(depths) is str: depths = [depths]
        if type(datatypes) is str: datatypes = [datatypes]
        if type(depths) is range: depths = [self.DEPTHS[i] for i in depths]
        #check if the arguments are correct, remove them otherwise
        # maptypes = [i for i in maptypes if (i in MAPTYPES)]
        # depths = [i for i in depths if (i in DEPTHS)]
        # datatypes = [i for i in datatypes if (i in DATATYPES)]
        #the number of requests from the server
        requests = len(maptypes)*len(depths)*len(datatypes)
        if(requests):
            print('Retrieving '+str(requests)+' maps with:')
            print(maptypes); print(depths); print(datatypes); print(bounds)
            paths = []
            
            pool = ThreadPool(len(maptypes))
            paths_objects = [pool.apply_async(self.getCube,
                                              args=(mtype,depths,datatypes,
                                                    bounds,overwrite))
                             for mtype in maptypes]
            
            paths += [path.get() for path in paths_objects]
            pool.close()
            pool.join()
            return paths
        else:
            print('invalid request:')
            print(maptypes)
            print(depths)
            print(datatypes)
            print(bounds) 
    


#%%
sd = SoilDownload()
bounds = gpd.read_file('/home/joempie/Documents/y2b2/LDD/ARCGIS_Files/fincha/data/Hadocha_mask.gpkg').to_crs(sd.WCRS).bounds.loc[0]
cubes = sd.getCubes(maptypes='all',depths='all',datatypes='all',bounds=bounds,overwrite=False)
import rioxarray
cubes = [[rioxarray.open_rasterio(fpath) for fpath in cube] for cube in cubes]
print('done!')
# rio.plot.show(ph, title='pH', cmap='viridis')
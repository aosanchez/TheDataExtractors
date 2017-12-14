"""
Tony Sanchez
10/20/2017
Georgetown Cohort 10: The Data Extractors Team!

Description:
This Python code was written to convert extracted Lat/Lon values
to a designated neighborhood_cluster, used in other parts of the project.
neighborhood_cluster is the shapefile grouping we are using to compare and contrast
each neighborhood to identify growth and decline.
"""

import shapefile
from shapely.geometry import Point, shape
"""
Returns neighborhood_cluster value as well as the description of the neighborhood.
"""
def getNeighborhoodClusterLatLon(path, lat, lon):
    # read your shapefile
    sf = shapefile.Reader(path)
    num_shapes = sf.numRecords
    # get the shapes
    shapes = sf.shapes()
    # format the point
    point = Point(lon, lat)
    # iterate through the shapes and check each polygon for the point
    for i in range(num_shapes):
        polygon = shape(shapes[i])
        if (polygon.contains(point)):
            cluster = sf.record(i)
            # return both the cluster neighborhood and neighborhood names
            return(cluster[2], cluster[3])
    return "Neighborhood not found."

if __name__ == '__main__':
    """
    Test code that loads a local shapefile and returns a neighborhood_cluster location
    """
    lat = 0
    lon = 0
    print("latitude is: " + str(lat))
    print("longitude is: " + str(lon))
    shp_file_base='Neighborhood_Clusters'
    dat_dir='/Users/anthonysanchez/Downloads/'+shp_file_base +'/'
    path = dat_dir+shp_file_base

    print("Neighborhood is: " + str(getNeighborhoodClusterLatLon(path,lat,lon)))

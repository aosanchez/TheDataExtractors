import pandas as pd
import checkLocation
import psycopg2
import psycopg2.extras

from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, Boolean, insert

def updateCrimeIncidentInfo(newclustervalue, neighborhood_desc, objectid_value):
    stm1 = "UPDATE crime_incidents SET"
    stm2 = " neighborhood_cluster = " + "'" + newclustervalue + "'" + ","
    stm3 = " neighborhood_description = " + "'" + neighborhood_desc + "'"
    stm4 = " WHERE objectid = " + str(objectid_value)
    stm5 = stm1 + stm2 + stm3 + stm4
    print("Exectuting: " + stm5)
    engine.execute(stm5)

def insertNeighborhoodDescOld(nameTable):
    stmt = 'SELECT X, Y, neighborhood_cluster, adjusted_neighborhood_cluster, census_tract, BID, objectid, neighborhood_description FROM ' + nameTable
    #print("Exectuting: " + stmt)
    results = connection.execute(stmt).fetchall()
    counter = 0

    for item in results:
        ##Pointing to the X and Y points in each row
        lon = item[0]
        lat = item[1]
        newCluster = checkLocation.getNeighborhoodClusterLatLon(path, lat, lon)
        #Use function to replace and input new value into the neighborhood_cluster field & the neighborhood_description
        if newCluster[0] != item[2]:
            if item[2] == None:
                print(updateCrimeIncidentInfo(newCluster[0], newCluster[1], item[6]))
                print("Filled neighborhood_cluster to: " + newCluster[0])
            else:
                print("Clusters don't match.")
                print(updateCrimeIncidentInfo(newCluster[0], newCluster[1], item[6]))
                print("Old value: " + str(item[2]) + " is updated to " + newCluster[0])

            counter = counter + 1
            print("Counter is: " + str(counter))

def insertNeighborhoodDesc(nameTable):
    stmt1 = 'SELECT X, Y, neighborhood_cluster, adjusted_neighborhood_cluster, census_tract, BID, objectid, neighborhood_description FROM ' + nameTable
    results1 = connection.execute(stmt1).fetchall()
    counter = 0

    for row in results1:
        ##Pointing to the X and Y points in each row
        lon = row[0]
        lat = row[1]
        newCluster = checkLocation.getNeighborhoodClusterLatLon(path, lat, lon)
        if newCluster[0] != row[2]:
            if row[2] != None:
                print("START Object ID: " + str(row[6]))
                newcensustractupdate = censusTractToInt(row[4])
                if str(row[4]) != newcensustractupdate:
                    print("Clusters don't match: ")
                    print("Original: " + str(row[2]))
                    print("Tony's conversion: " + newCluster[0])
                    print("Jay's conversion: " + newcensustractupdate)

                    counter = counter + 1
                    print("Counter is: " + str(counter))
                    print("---------------------------" )


def censusTractToInt(censusTractNum):
    tract = str(censusTractNum)
    print("Orig tract num: " + tract)
    length = len(tract)
    if length > 2:
        if tract[-2:length] == "00":
            tract = tract[0:length-2]
            print("IN Here: " + tract)
        else:
            tract = tract[0:2] +"."+tract[2:length]
    else:
        print("Did not assign a tract!!")

    newtract = "Census Tract " + tract
    print("New tract is: " + tract)

    stmt3 = 'SELECT * from census_tracts where census = ' + "'" +newtract+"'"
    print(stmt3)
    results3 = connection.execute(stmt3).fetchall()
    return (results3[0][3])


if __name__ == '__main__':
    engine = create_engine('postgres://Tony:Sanchez@de-dbinstance.c6dfmakosb5f.us-east-1.rds.amazonaws.com:5432/dataextractorsDB')
    connection = engine.connect()
    metadata = MetaData(engine)

    """
    print(engine.table_names())
    print()

    lat = 38.8954982072441
    lon = -77.0302019597989
    print("latitude is: " + str(lat))
    print("longitude is: " + str(lon))
    """
    shp_file_base='Neighborhood_Clusters'
    dat_dir='/Users/anthonysanchez/Downloads/'+shp_file_base +'/'
    path = dat_dir+shp_file_base

    print ("Processing............ ")
    insertNeighborhoodDescOld('crime_incidents')

    ##print("Neighborhood is: " + str(getNeighborhoodClusterLatLon(path,lat,lon)))


    ###readTable('crime_incidents')
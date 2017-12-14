"""
Tony Sanchez
11/20/2017
Georgetown Cohort 10: The Data Extractors Team

Description:
This is Python code generates a comprehensive crime_incidents DF that
allows an analyst to do computations on the results focusing on:
Date Range, Neighborhood and Crime Incidents
"""
#import loadCrimeIncidents as lc
import datetime as dt
import pandas as pd
import numpy as np
from sqlalchemy import create_engine

""" Function returns Main Query path to the Postgress DB """
def returnSQLQuery():
    ##query = 'SELECT report_date, shift, method, offence, x, y, neighborhood_cluster, neighborhood_clusters.nbh_name FROM crime_incidents'
    ###join = ' INNER JOIN neighborhood_clusters on crime_incidents.neighborhood_cluster = neighborhood_clusters."Name"'
    ###both = query + join
    query = 'SELECT report_date, neighborhood_cluster, offence, method, shift FROM crime_incidents'
    #query = 'SELECT neighborhood_cluster, report_date, shift, method, offence FROM crime_incidents'
    query2 = 'SELECT "Name" FROM neighborhood_clusters'
    return(query, query2)

""" Function returns our DBs Main DataFrame on crime_incidents """
def getDataFrame(path, crimeIcidQuery, nhClusterQuery):
    engine = create_engine(path)
    df = pd.read_sql_query(crimeIcidQuery, engine, parse_dates=['report_date'])
    df2 = pd.read_sql_query(nhClusterQuery, engine)
    return(df, df2)

""" Adds the year_month column """
def addYearMonthCol(df):
    df['year_month'] = df['report_date'].apply(lambda x: x.strftime('%Y-%m'))
    return df

""" Returns new DataFrame with just the copied neighborhood_cluster and year_month info """
def createNewDF(df):
    df2 = pd.DataFrame()
    df2['n_cluster'] = df['neighborhood_cluster']
    df2['year_month'] = df['year_month']
    return df2

""" Create empty DF with neighborhood_cluster and year_month values input, the rest are zeros """
def getEmptyRowDF(nc, ym):
    s1 = pd.Series([nc, ym, 0, 0 , 0 , 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0])
    df = pd.DataFrame([list(s1)], columns=['n_cluster', 'year_month', 'num_crimes', 'DAY', 'EVENING', 'MIDNIGHT', 'ARSON', 'ASSAULT W/DANGEROUS WEAPON', 'BURGLARY', 'HOMICIDE', 'MOTOR VEHICLE THEFT', 'ROBBERY', 'SEX ABUSE', 'THEFT F/AUTO', 'THEFT/OTHER', 'GUN', 'KNIFE', 'OTHERS'])
    return df

""" Calls all functions that help return a base DataFrame from our SQL DB on crime_incidents """
def getCrimeIncidentDB(path):
    # Creat DataFrame from SQL crime_incidents database
    crimeIcidQuery, nhClusterQuery = returnSQLQuery()
    df, df2 = getDataFrame(path, crimeIcidQuery, nhClusterQuery)
    # Add a year_month column
    df = addYearMonthCol(df)
    #Reorder Neighborhood DF
    df2 = df2.sort_values(by='Name')
    df2 = df2.reset_index(drop=True)
    return(df, df2)

""" Creates a new Base DF with original values to be kept and groups them """
def createBaseDF(df):
    # Create Empty DataFrame with original main values
    df2 = createNewDF(df)
    # Groupby Neighborhood and Year/month with total num incidents
    df2 = df2.groupby(['n_cluster', 'year_month']).size()
    # Convert Series to a DataFrame
    df3 = pd.DataFrame(df2)
    # Normalize Index and rename columns
    df3.reset_index(inplace=True)
    df3.columns = ['n_cluster', 'year_month', 'num_crimes']
    return df3

""" Creats a Shift parameter DF to add to the BASE DF """
def createShiftDF(df):
    # Create New DF for Shift values
    dfShift = df.groupby(['neighborhood_cluster','year_month','shift']).size()
    # Unpack Shift values
    dfShift = dfShift.unstack(level='shift')
    dfShift.reset_index(inplace=True)
    # rename columns
    dfShift = dfShift.rename(columns={'neighborhood_cluster':'n_cluster'})
    # Convert NaN to 0s
    dfShift.fillna(0, inplace=True)
    #convert floats to ints
    dfShift['DAY'] = dfShift['DAY'].apply(np.int64)
    dfShift['EVENING'] = dfShift['EVENING'].apply(np.int64)
    dfShift['MIDNIGHT'] = dfShift['MIDNIGHT'].apply(np.int64)
    return dfShift

""" Creats n Offense parameter DF to add to the BASE DF """
def createOffenseDF(df):
    # Create New DF for Shift values
    dfOffense = df.groupby(['neighborhood_cluster','year_month','offence']).size()
    # Unpack Shift values
    dfOffense = dfOffense.unstack(level='offence')
    dfOffense.reset_index(inplace=True)
    # rename columns to merge
    dfOffense = dfOffense.rename(columns={'neighborhood_cluster':'n_cluster', 'offence':'offense'})
    # Convert NaN to 0s
    dfOffense.fillna(0, inplace=True)
    #convert floats to ints
    dfOffense['ARSON'] = dfOffense['ARSON'].apply(np.int64)
    dfOffense['ASSAULT W/DANGEROUS WEAPON'] = dfOffense['ASSAULT W/DANGEROUS WEAPON'].apply(np.int64)
    dfOffense['BURGLARY'] = dfOffense['BURGLARY'].apply(np.int64)
    dfOffense['HOMICIDE'] = dfOffense['HOMICIDE'].apply(np.int64)
    dfOffense['MOTOR VEHICLE THEFT'] = dfOffense['MOTOR VEHICLE THEFT'].apply(np.int64)
    dfOffense['ROBBERY'] = dfOffense['ROBBERY'].apply(np.int64)
    dfOffense['SEX ABUSE'] = dfOffense['SEX ABUSE'].apply(np.int64)
    dfOffense['THEFT F/AUTO'] = dfOffense['THEFT F/AUTO'].apply(np.int64)
    dfOffense['THEFT/OTHER'] = dfOffense['THEFT/OTHER'].apply(np.int64)
    return dfOffense

""" Creats a Method parameter DF to add to the BASE DF """
def createMethodDF(df):
    # Create New DF for Shift values
    dfMethod = df.groupby(['neighborhood_cluster','year_month','method']).size()
    # Unpack Shift values
    dfMethod = dfMethod.unstack(level='method')
    dfMethod.reset_index(inplace=True)
    # rename columns
    dfMethod = dfMethod.rename(columns={'neighborhood_cluster':'n_cluster'})
    # Convert NaN to 0s
    dfMethod.fillna(0, inplace=True)
    #convert floats to ints
    dfMethod['GUN'] = dfMethod['GUN'].apply(np.int64)
    dfMethod['KNIFE'] = dfMethod['KNIFE'].apply(np.int64)
    dfMethod['OTHERS'] = dfMethod['OTHERS'].apply(np.int64)
    return dfMethod

""" Returns a new merged DF with the previous and new DF """
def mergeDFs(BaseDF, AddDF):
    #merge base and new DF
    newDF = pd.merge(BaseDF, AddDF, how='left', on=['n_cluster','year_month'])
    return newDF

"""
Adjust the Date Range of the selected data
To be applied on original DF before conversion
"""
def maskDateDF(df, frm, to):
    date_mask = (df.year_month >= frm) & (df.year_month < to)
    dates = df.index[date_mask]
    df1 = df.loc[dates]
    df1.reset_index(inplace=True)
    #df1.reset_index(inplace=True, drop=True)
    return df1

"""
Main function to call other functions to build core
crime_incidents DataFrame.
"""
def getBaseDF(path, frm, to):
    origDF, nhDF = getCrimeIncidentDB(path)
    #Mask the unwanted dates ** MUST DO THIS HERE **
    origDF2 = maskDateDF(origDF, frm, to)
    #combine with base
    baseDF = createBaseDF(origDF2)
    #Merge Shift features
    shiftDF = createShiftDF(origDF2)
    baseDF2 = mergeDFs(baseDF, shiftDF)
    #Merge Offense features
    offenseDF = createOffenseDF(origDF2)
    baseDF3 = mergeDFs(baseDF2, offenseDF)
    #Merge Method features
    methodDF = createMethodDF(origDF2)
    baseDF4 = mergeDFs(baseDF3, methodDF)
    return(baseDF4, nhDF)


""" Function that fills table with empty values other than the n_cluster and year_month """
def insertEmptyRows(baseDF, nhDF, drDF):
    n_len = len(nhDF) #Number of neighborhood clusters
    d_len = len(drDF) #Number of date/months in a range
    index_inc = 0

    for i in range(n_len): #for every neighborhood
        dfNbh = baseDF[baseDF['n_cluster'] == nhDF.Name[i]] #get's cluster group
        if dfNbh['year_month'].count() < d_len:
            for t in range(d_len): #for every month, check cluster range for missing dates and insert
                dfMnt = dfNbh[dfNbh['year_month'] == drDF[0][t]]
                if dfMnt.empty:
                    addDF = getEmptyRowDF(nhDF.Name[i], drDF[0][t])
                    baseDF = baseDF.append(addDF)
                    baseDF.reset_index(inplace=True, drop=True)
                    ## Re-sort base DF here to list the added rows higher
                index_inc = index_inc + 1
        else:
            index_inc = index_inc + d_len
    #sort DF by neighborhood and year_month. Gets added rows inline with other year_months
    baseDF2 = baseDF.sort_values(by=['n_cluster', 'year_month'])
    #reset the row index values and
    baseDF2.reset_index(inplace=True, drop=True)
    #check value to verify total row count
    #print("The total index count is: " + str(index_inc)) #check value to verify total row count
    return baseDF2

""" Returns num years covered """
def getDateRangeDF(frm, to):
    #Parsed date/month and total values.
    drange = pd.date_range(start=frm,end=to, freq='M')
    df = pd.DataFrame(drange)
    df[0] = df[0].apply(lambda x: x.strftime('%Y-%m'))
    return df

""" mergeLikeColumns: Merges crime columns into two: Violent Crime and Theft Crime
    Function simply adds freqencies from each like column into one.
    note: I decided to leave this in the raw load file for now
    since we wanted the most relevent simple DF output at this points
    of the workflow.
"""
def mergeLikeColumns(df):
    #remove unused columns
    tempDF = df.drop(['DAY', 'EVENING', 'MIDNIGHT', 'GUN', 'KNIFE', 'OTHERS'], axis=1)
    #create two columns that represent violent crime and Theft
    #violent crime inludes: ARSON, ASSAULT W/DANGEROUS WEAPON, HOMICIDE, SEX ABUSE,
    #Theft includes: BURGLARY, MOTOR VEHICLE THEFT, ROBBERY, THEFT F/AUTO, THEFT/OTHER
    violentCM = df['ARSON'] + df['ASSAULT W/DANGEROUS WEAPON'] + df['HOMICIDE'] + df['SEX ABUSE']
    theftCM = df['BURGLARY'] + df['MOTOR VEHICLE THEFT'] + df['ROBBERY'] + df['THEFT F/AUTO'] + df['THEFT/OTHER']
    #Adjust these two columns and make a ratio
    tempDF['violent_cm'] = violentCM
    tempDF['theft_cm'] = theftCM
    #tempDF['V/T Ratio'] = violentCM/theftCM
    newDF = tempDF.drop(['ARSON','ASSAULT W/DANGEROUS WEAPON','BURGLARY','HOMICIDE','MOTOR VEHICLE THEFT','ROBBERY','SEX ABUSE','THEFT F/AUTO','THEFT/OTHER'], axis=1)
    return newDF

def runLCI(path, frm, to):
    print("Building the simplified raw Crime DataFrame...")
    #returns full date range expected per year. i.e. Jan-Dec
    drDF = getDateRangeDF(frm,to)
    #returns full crime_incidents DF with n_cluster DF
    baseDF, nhDF = getBaseDF(path, frm, to)
    #pass in both DFs to get final edited DF
    rawDF = insertEmptyRows(baseDF, nhDF, drDF)
    #merge like columns
    lciDF = mergeLikeColumns(rawDF)
    return lciDF, nhDF

if __name__ == '__main__':
    """ Main test code to kick off DataFrame build process """
    # set pass in params to specific years of coverage and SQL DB
    print("Kick off the basic DF gen process...")

"""
Tony Sanchez
11/23/2017
Georgetown Cohort 10: The Data Extractors Team

Description:
This is Python code generates all of my statistics per monthly
and per year for each neighborhood_cluster. It also reformats the DataFrame for
Crime, adds Scores Column and Calculates Total Scores for all Categories in our
Capstone project.
import computationAnalysis as ca
"""

import pandas as pd
import numpy as np
import loadCrimeIncidents as lc
from natsort import natsorted

def getStatsNeighborhood(df, nhDF, frm, to):
    #Get date range
    #drange = lc.getDateRangeDF(frm, to)
    #Get the basic time differences
    yd, md, fy, ty, fm, tm = getDateDiff(frm, to)
    firstyr_str = retEndFYStr(fy, fm)
    lastyr_str = retBegLYStr(ty, tm)

    #Set up return DF with added columns for mean, med and perc diff
    nhDF['n_mean'] = nhDF.apply(lambda _: '', axis=1)
    nhDF['n_med'] = nhDF.apply(lambda _: '', axis=1)
    nhDF['fy_mean'] = nhDF.apply(lambda _: '', axis=1)
    nhDF['ly_mean'] = nhDF.apply(lambda _: '', axis=1)
    nhDF['perc_growth'] = nhDF.apply(lambda _: '', axis=1)
    nhDF.columns = ['n_cluster','mean','med','fy_mean','ly_mean','perc_growth']

    for index, row in nhDF.iterrows():
        #for each row, extract each neighborhood_cluster
        ncDF = df[df['n_cluster'] == nhDF.n_cluster[index]]
        #calc mean and med for the entire timeline
        nhDF['mean'][index] = ncDF.num_crimes.mean()
        nhDF['med'][index] = ncDF.num_crimes.median()
        #get mean of first and last years to compare
        first_yearDF = getYearlyData(ncDF, frm, firstyr_str)
        last_yearDF = getYearlyData(ncDF, lastyr_str, to)
        nhDF['fy_mean'][index] = first_yearDF.num_crimes.mean()
        nhDF['ly_mean'][index] = last_yearDF.num_crimes.mean()
        # Calculate percent difference between first and last years
        nhDF['perc_growth'][index] = getPercDiff(first_yearDF.num_crimes.mean(), \
            last_yearDF.num_crimes.mean())

    return nhDF

def getStatsWashDC(df, frm, to):
    #Get the basic time differences
    yd, md, fy, ty, fm, tm = getDateDiff(frm, to)
    # Calc total city wide mean
    total_citycrime_mean = df.num_crimes.mean()
    """ Calculating Growth from first and last year of date range """
    #mean of first year
    firstyr_str = retEndFYStr(fy, fm)
    first_yearDF = getYearlyData(df, frm, firstyr_str)
    #mean of last year
    lastyr_str = retBegLYStr(ty, tm)
    last_yearDF = getYearlyData(df, lastyr_str, to)
    #Caclulate Growth over the time period
    growth = getPercDiff(first_yearDF.num_crimes.mean(), \
        last_yearDF.num_crimes.mean())

    print("Number of Crimes in DC. " + str(fy) + ": " + str(first_yearDF.num_crimes.sum()))
    print("Number of Crimes in DC. " + str(ty) + ": " + str(last_yearDF.num_crimes.sum()))
    print("Violent Crimes in DC. " + str(fy) + ": " + str(first_yearDF.violent_cm.sum()))
    print("Violent Crimes in DC. " + str(ty) + ": " + str(last_yearDF.violent_cm.sum()))
    print("Theft Crimes in DC. " + str(fy) + ": " + str(first_yearDF.theft_cm.sum()))
    print("Theft Crimes in DC. " + str(ty) + ": " + str(last_yearDF.theft_cm.sum()))
    #print(first_yearDF)
    #print(last_yearDF)
    return(total_citycrime_mean, growth)

#Returns a string with the end of the first year for stats
def retEndFYStr(fy, fm):
    fy = fy + 1
    if fm > 9:
        first_year_to = str(fy) + "-" + str(fm)
    else:
        first_year_to = str(fy) + "-0" + str(fm)
    return first_year_to

#Returns a string with the beginning of the last year for stats
def retBegLYStr(ty, tm):
    ty = ty - 1
    if tm > 9:
        first_year_frm = str(ty) + "-" + str(tm)
    else:
        first_year_frm = str(ty) + "-0" + str(tm)
    return first_year_frm

def getPercDiff(start, end):
    if end == 0:
        return 0
    else:
        growth = ((end - start) / end) * 100
        return growth

def getCityStatsDC(df):
    citydf = df.describe()
    return citydf

def getYearlyData(df, frm, to):
    yrDF = df[(df['year_month'] >= frm) & (df['year_month'] < to)]
    return yrDF

def getMonthlyValues(df, month):
    dfMnt = df[df['year_month'] == month]

def getNeighborhoodValues(df, nbh):
    dfNbh = df[df['n_cluster'] == nbh]

def getNeighorhoodClusterDF(path):
    engine = create_engine(path)
    nhClusterQuery = query2 = 'SELECT "Name" FROM neighborhood_clusters'
    df2 = pd.read_sql_query(nhClusterQuery, engine)
    #Reorder Neighborhood DF
    df2 = df2.sort_values(by='Name')
    df2 = df2.reset_index(drop=True)
    return(df2)

#Params that may be needed later.
def getDateDiff(frm, to):
    fy = int(frm[:4])
    ty = int(to[:4])
    fm = int(frm[5:])
    tm = int(to[5:])
    yearsdiff = (ty-fy)
    monthsdiff = (tm-fm)
    return (yearsdiff, monthsdiff, fy, ty, fm, tm)

""" Adds a column that shows the distance from the city wide monthly num_crimes mean and the
    actual per neighborhood month value m_diff. q_score is used to label ML clusters """
def addMonthlyScoreColumn(df, drdf):
    for i in range(len(drdf)):
        #create mask for each month in time period
        mask = df['year_month'].str.match(drdf[0][i])
        mask_index = df[mask].index
        m_crime = df[mask]['num_crimes'].mean()
        # Calculate and set difference from crime to mean
        df.loc[mask_index, 'crime_diff'] = df[mask]['num_crimes'] - m_crime
        #Q score for crime data. Score is reversed to reflect growth/positive for crime
        q_crime = pd.qcut(df[mask]['crime_diff'], 9, labels=[4, 3, 2, 1, 0, -1, -2, -3, -4])
        df.loc[mask_index, 'CrimeQScore'] = q_crime
    return(df)

    """ Older code for this function
    for i in range(len(df)):
        #get the mean of all clusters per month
        citywide_ym = df[(df['year_month'] == df['year_month'][i])].num_crimes.mean()
        #subract mean from actual for score
        df.loc[i,'crime_diff'] = df['num_crimes'][i] - citywide_ym

    qmi = pd.qcut(df['crime_diff'], 9, labels=[4, 3, 2, 1, 0, -1, -2, -3, -4])
    df['CrimeQScore'] = qmi
    return df
    """

def addNHScoreQcut(df, citywide_growth):
    #creat series array of perc_growth values
    s_perc_growth = df['perc_growth']
    #subtract the city wide growth values from each nh value
    growth = s_perc_growth - citywide_growth
    #apply qcut. *** Notice I reversed the order of the +4 to -4 to reflect
    #growth as a decline in crime
    qmi = pd.qcut(s_perc_growth, 9, labels=[4, 3, 2, 1, 0, -1, -2, -3, -4])
    #add column wiht scores to the data frame
    df['n_score'] = qmi
    return(df)

""" Two functions written to remove NH Cluster Rows """
def removeUnwantedNHs(df, nhdf, nh):
    #for the base dataframe
    df1 = df.set_index('n_cluster')
    df2 = df1.drop(nh)
    df2.reset_index(inplace=True)
    #for the neighborhood_cluster dataframe
    nhdf1 = nhdf.set_index('Name')
    nhdf2 = nhdf1.drop(nh)
    nhdf2.reset_index(inplace=True)
    return(df2,nhdf2)

""" load externa CSV to build score table. """
def loadCSV(df, path):
    #Get Ken's Data
    path2 = path + 'SalesPrice.csv'
    df2 = pd.read_csv(path2)
    sortby = "nbh_cluster"
    df3 = sortDF(df2, sortby)
    #set scores
    qmi3 = pd.qcut(df3['NBH_Distance_From_DC_Mean'], 9, labels=[-4, -3, -2, -1, 0, 1, 2, 3, 4])
    dfq = pd.DataFrame(qmi3)

    #get Jays and Jason's data
    path3 = path + 'scores1.csv'
    mainDF = pd.read_csv(path3)
    #add ken's sales data
    mainDF['SalesPrice'] = dfq

    #get the crime data
    sortby = "n_cluster"
    crimeDF = sortDF(df, sortby)
    dfqc = pd.DataFrame(crimeDF['n_score'])
    mainDF['Crime'] = dfqc

    #['NBH_Distance_From_DC_Mean']
    total_score = mainDF['Population'] + mainDF['Poverty'] + mainDF['Mean Income'] + mainDF['SalesPrice'] + mainDF['Crime']
    mainDF['TotalScore'] = total_score
    qmi4 = pd.qcut(mainDF['TotalScore'], 5, labels=['Facing the Greatest Challenges', 'Falling Behind', 'Average', 'Advancing', 'Making the Greatest Advances'])
    mainDF['Classification'] = qmi4
    return(mainDF)

def sortDF(df,sortby):
    sorter = natsorted(df[sortby])
    df[sortby] = df[sortby].astype("category")
    df[sortby].cat.set_categories(sorter, inplace=True)
    df = df.sort_values([sortby])
    df.reset_index(inplace=True, drop=True)
    return df

#returns two DataFrames Used in Machine Learning and Visualization
def getCrimeAndCategoryTotals(path, frm, to, nh_to_rem, file_path):
    #Main raw DF gen code from loadCrimeIncidents
    df, nhDF = lc.runLCI(path, frm, to)
    #code to remove unwanted clusters we want to exclude from
    #both DFs before we perform any calculations
    #set the desired neighborhood_clusters to remove from future calcs
    print("Editing the raw DataFrame to add Monthly Crime Scores, Remove Neighborhoods...")
    df2, nhDF2  = removeUnwantedNHs(df, nhDF, nh_to_rem)
    #Add Montly Score column for num_crimes difference from monthly mean, passing in date range
    drdf = lc.getDateRangeDF(frm, to)
    finalDF = addMonthlyScoreColumn(df2, drdf)
    #print(finalDF)

    print("Building the Crime and Team Total Scores DataFrame....")
    #returns total city num_crimes mean adn citywide growth over desired period
    total_citycrime_mean, citywide_growth = getStatsWashDC(finalDF, frm, to)
    print("DC Crime mean overall neighborhoods: " + str(round(total_citycrime_mean,2)))
    print("DC Crime growth between first year and last year: " + str(round(citywide_growth, 2)) + "%")
    #retuns a New DF that calculates the score for the crime category
    new_nhdf = getStatsNeighborhood(finalDF, nhDF2, frm, to)
    crime_scoresDF = addNHScoreQcut(new_nhdf, citywide_growth)
    #Three calls to get final processed Crime DataFrame
    #returns full date range expected
    TotalScoresDF = loadCSV(crime_scoresDF, file_path)
    #print(TotalScoresDF)
    return(finalDF, TotalScoresDF)

if __name__ == '__main__':
    #Set the pass in variables
    path = "Insert DB path here"
    frm = '2011-01'
    to = '2016-01'
    nh_to_rem = ['Cluster 45', 'Cluster 46']
    file_path = '/Users/anthonysanchez/Downloads/'
    c_file = file_path + 'crime_df.csv'
    ts_file = file_path + 'total_scores_df.csv'
    q_file = file_path + 'q_df.csv'

    CrimeDF, TotalScoresDF = getCrimeAndCategoryTotals(path,frm,to,nh_to_rem,file_path)

    #print(CrimeDF)
    #print(TotalScoresDF)
    #CrimeDF.to_csv(c_file)
    #TotalScoresDF.to_csv(ts_file)

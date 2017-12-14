"""
Tony Sanchez
11/23/2017
Georgetown Cohort 10: The Data Extractors Team

Description:
This code was used to verify our data by generating yearly stats data that would
show percent change yearly instead of monthly.
"""

import pickle
import pandas as pd
import numpy as np
import loadCrimeIncidents as lc
from natsort import natsorted
import datetime as dt

def getBaseYBY_DF(p_file):
    teamDF = pickle.load(open(p_file, "rb"))
    #teamDF.to_csv('/Users/anthonysanchez/Downloads/teamDF_input.csv')
    newDF = teamDF.drop(['Median Price Asked', 'Total Crimes', 'Violent Crimes', 'Theft Crimes'], axis=1)
    df = newDF.reset_index()
    df['Date'] = df['Date'].apply(lambda x: x[0:4])
    df = df.drop_duplicates()
    df = df.reset_index(level=False, drop=True)

    #do the math
    newDF = teamDF.drop(['Population', 'Population', 'White', 'Black', 'Asian/Pacific','Native American','Dependency Ratio', 'M/F Ratio', 'Own/Rent Ratio', 'HS Max %', 'College Educated %', 'Naturalized %','No Citizen %', 'Poverty Below 100', 'Poverty 100-149', 'Native Born %', 'Mean Income', 'Median Rent Price'],axis=1)
    df2 = newDF.reset_index()
    df2['Date'] = df2['Date'].apply(lambda x: x[0:4])
    df2.columns = ['Date','Cluster','MedPriceAsked','TotalCrimes','ViolentCrimes','TheftCrimes']

    cluster = df['Cluster']
    cluster = cluster.drop_duplicates()
    cluster = cluster.reset_index(level=False, drop=True)
    year = ['2011', '2012', '2013', '2014', '2015']

    i = 0
    for x in range(len(cluster)):
        for y in range(len(year)):
            value = round(df2[(df2['Date'] == year[y]) & (df2['Cluster'] == cluster[x])].MedPriceAsked.mean(),2)
            df.loc[i, 'MedPriceAsked'] = value
            i = i + 1

    i = 0
    for x in range(len(cluster)):
        for y in range(len(year)):
            value = df2[(df2['Date'] == year[y]) & (df2['Cluster'] == cluster[x])].TotalCrimes.sum()
            df.loc[i, 'TotalCrimes'] = value
            i = i + 1

    i = 0
    for x in range(len(cluster)):
        for y in range(len(year)):
            value = df2[(df2['Date'] == year[y]) & (df2['Cluster'] == cluster[x])].ViolentCrimes.sum()
            df.loc[i, 'ViolentCrimes'] = value
            i = i + 1

    i = 0
    for x in range(len(cluster)):
        for y in range(len(year)):
            value = df2[(df2['Date'] == year[y]) & (df2['Cluster'] == cluster[x])].TheftCrimes.sum()
            df.loc[i, 'TheftCrimes'] = value
            i = i + 1

    #df.to_csv('/Users/anthonysanchez/Downloads/teamDF_output.csv')

    return df


def getPercChangeDF(df):
    pcDF = pd.DataFrame()
    cluster = df['Cluster']
    cluster = cluster.drop_duplicates()
    cluster = cluster.reset_index(level=False, drop=True)
    year_range = ['2011to2012', '2012to2013', '2013to2014', '2014to2015']

    i=0
    for x in range(len(cluster)):
        for y in range(len(year_range)):
            pcDF.loc[i, 'Period'] = year_range[y]
            pcDF.loc[i, 'Cluster'] = cluster[x]
            i = i + 1

    i=0
    for x in range(len(cluster)):
        tempDF = df[df['Cluster']==cluster[x]]
        tempDF.reset_index(inplace=True, drop=True)
        for y in range(len(year_range)):
            location = "Population Column, " + cluster[x]
            pcDF.loc[i,'PopPD'] = getPercDiff(location, tempDF['Population'][y+1], tempDF['Population'][y])

            location = "White Column, " + cluster[x]
            pcDF.loc[i,'WhitePD'] = getPercDiff(location, tempDF['White'][y+1], tempDF['White'][y])

            location = "Black Column, " + cluster[x]
            pcDF.loc[i,'BlackPD'] = getPercDiff(location, tempDF['Black'][y+1], tempDF['Black'][y])

            location = "Asian/Pacific Column, " + cluster[x]
            pcDF.loc[i,'AsianPacificPD'] = getPercDiff(location, tempDF['Asian/Pacific'][y+1], tempDF['Asian/Pacific'][y])

            location = "Native American Column, " + cluster[x]
            pcDF.loc[i,'NativeAmericanPD'] = getPercDiff(location, tempDF['Native American'][y+1], tempDF['Native American'][y])

            location = "Dependency Ratio Column, " + cluster[x]
            pcDF.loc[i,'DependencyRatioPD'] = getPercDiff(location, tempDF['Dependency Ratio'][y+1], tempDF['Dependency Ratio'][y])

            location = "M/F Ratio Column Column, " + cluster[x]
            pcDF.loc[i,'M/F_RatioPD'] = getPercDiff(location, tempDF['M/F Ratio'][y+1], tempDF['M/F Ratio'][y])

            location = "Own/Rent Ratio Column, " + cluster[x]
            pcDF.loc[i,'Own/Rent_RatioPD'] = getPercDiff(location, tempDF['Own/Rent Ratio'][y+1], tempDF['Own/Rent Ratio'][y])

            location = "Naturalized Perc Column, " + cluster[x]
            pcDF.loc[i,'NaturalizedPercPD'] = getPercDiff(location, tempDF['Naturalized %'][y+1], tempDF['Naturalized %'][y])

            location = "No Citizen Perc Column, " + cluster[x]
            pcDF.loc[i,'NoCitizenPercPD'] = getPercDiff(location, tempDF['No Citizen %'][y+1], tempDF['No Citizen %'][y])

            location = "Poverty Below 100 Column, " + cluster[x]
            pcDF.loc[i,'PovertyBelow100PD'] = getPercDiff(location, tempDF['Poverty Below 100'][y+1], tempDF['Poverty Below 100'][y])

            location = "Poverty 100-149 Column, " + cluster[x]
            pcDF.loc[i,'Poverty 100-149PD'] = getPercDiff(location, tempDF['Poverty 100-149'][y+1], tempDF['Poverty 100-149'][y])

            location = "Mean Income Column, " + cluster[x]
            pcDF.loc[i,'MeanIncomePD'] = getPercDiff(location, tempDF['Mean Income'][y+1], tempDF['Mean Income'][y])

            location = "Median Rent Price Column , " + cluster[x]
            pcDF.loc[i,'MedianRentPricePD'] = getPercDiff(location, tempDF['Median Rent Price'][y+1], tempDF['Median Rent Price'][y])

            location = "MedPriceAsked Column, " + cluster[x]
            pcDF.loc[i,'MedPriceAskedPD'] = getPercDiff(location, tempDF['MedPriceAsked'][y+1], tempDF['MedPriceAsked'][y])

            location = "TotalCrimes Column, " + cluster[x]
            pcDF.loc[i,'TotalCrimesPD'] = getPercDiff(location, tempDF['TotalCrimes'][y+1], tempDF['TotalCrimes'][y])

            location = "ViolentCrimes Column, " + cluster[x]
            pcDF.loc[i,'ViolentCrimesPD'] = getPercDiff(location, tempDF['ViolentCrimes'][y+1], tempDF['ViolentCrimes'][y])

            location = "TheftCrimes Column, " + cluster[x]
            pcDF.loc[i,'TheftCrimesPD'] = getPercDiff(location, tempDF['TheftCrimes'][y+1], tempDF['TheftCrimes'][y])
            i = i + 1

    return pcDF


def getPercDiff(location, start, end):
    if start == end == 0:
        print("Both start value and end value are zeros in " + location)
        return 0
    elif end == 0:
        print("The end value caused a divide by zero error in " + location)
        return 100
    else:
        growth = ((end - start) / end) * 100
        return growth


if __name__ == '__main__':
    #Set the pass in variables
    path = ""
    frm = '2011-01'
    to = '2016-01'
    nh_to_rem = ['Cluster 45', 'Cluster 46']
    file_path = '/Users/anthonysanchez/Downloads/'
    c_file = file_path + 'crime_df.csv'
    ts_file = file_path + 'total_scores_df.csv'
    q_file = file_path + 'q_df.csv'
    p_file = file_path + 'ml.p'

    bdf = getBaseYBY_DF(p_file)
    pcDF = getPercChangeDF(bdf)
    #bdf.to_csv('/Users/anthonysanchez/Downloads/TeamBaseYbY_DF.csv')
    #pcDF.to_csv('/Users/anthonysanchez/Downloads/TeamPercChg_DF.csv')

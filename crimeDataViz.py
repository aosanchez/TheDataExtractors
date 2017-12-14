"""
Tony Sanchez
11/28/2017
Georgetown Cohort 10: The Data Extractors Team

Description:
This is Python code helps vizualize crime incident data in different ways.
"""

import pandas as pd
import numpy as np
import loadCrimeIncidents as lc
import computationAnalysis as ca
import matplotlib.pyplot as plt
import matplotlib.cm as cm
from mpl_toolkits.mplot3d import Axes3D
import seaborn as sns
import time
import math
from pandas.plotting import scatter_matrix, parallel_coordinates, andrews_curves, autocorrelation_plot,radviz, lag_plot
from sqlalchemy import create_engine


def getNHDataFrame(path):
    engine = create_engine(path)
    query = 'SELECT "Name", "nbh_name" FROM neighborhood_clusters'
    df = pd.read_sql_query(query, engine)
    return(df)

def addNHDesc(df, ndf):
    ndf2 = ndf.set_index('Name')
    ndf2.sort_index()
    df['nbh_name'] = df['n_cluster'].apply(lambda x: ndf2.loc[x].nbh_name)
    return(df)

def runBox(df):
    ax = sns.boxplot(x="nbh_name", y="num_crimes", data=df)
    ax.set_xticklabels(ax.get_xticklabels(), rotation=90)
    plt.show()

def runScatter(df):
    scatter_matrix(df, diagonal='kde')
    plt.show()

def getOrigDF(path):
    origDF, nhDF = lc.getCrimeIncidentDB(path)
    return (origDF, nhDF)

if __name__ == '__main__':
    print("Starting Stats Gen...")
    path = 'Insert DB path here'
    frm = '2010-01'
    to = '2017-01'

    drDF = lc.getDateRangeDF(frm, to)
    #returns full crime_incidents DF with n_cluster DF
    baseDF, nhDF = lc.getBaseDF(path, frm, to)
    #pass in both DFs to get final edited DF
    df = lc.insertEmptyRows(baseDF, nhDF, drDF)

    ndf = getNHDataFrame(path)
    fin_df = addNHDesc(df, ndf)


    total_citycrime_mean, citywide_growth = ca.getStatsWashDC(df, frm, to)
    print("DC overall crime mean: " + str(total_citycrime_mean))
    print("DC growth over period: " + str(citywide_growth))
    new_nhdf = ca.getStatsNeighborhood(df, nhDF, frm, to)
    scoresDF = ca.addNeighborhoodScore(new_nhdf, citywide_growth)

    print(scoresDF)

# Author:   Jay Huang <askjayhuang@gmail.com>
# Created:  October 20, 2017

"""A module for reading csv files and wrangling data."""

################################################################################
# Imports
################################################################################

import pandas as pd
import pickle
import glob
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, Float, Boolean, insert
import pg

################################################################################
# Functions
################################################################################


def create_char_table():
    """Read general characteristics csv file and impute missing data."""
    files = glob.glob('data/char_*.csv')
    dfs = []

    for f in files:
        i = f.find('20')
        yr = f[i:i + 4]

        if yr == '2013' or yr == '2014' or yr == '2015':
            df = pd.read_csv(f, header=0, skiprows=[1], usecols=[
                             'GEO.id', 'GEO.display-label', 'HC01_EST_VC01', 'HC01_EST_VC03', 'HC01_EST_VC04', 'HC01_EST_VC05', 'HC01_EST_VC06', 'HC01_EST_VC07', 'HC01_EST_VC08', 'HC01_EST_VC09', 'HC01_EST_VC10', 'HC01_EST_VC11', 'HC01_EST_VC16', 'HC01_EST_VC17', 'HC01_EST_VC21', 'HC01_EST_VC22', 'HC01_EST_VC23', 'HC01_EST_VC24', 'HC01_EST_VC25', 'HC01_EST_VC26',
                             'HC01_EST_VC27', 'HC01_EST_VC33', 'HC01_EST_VC35', 'HC01_EST_VC36', 'HC01_EST_VC40', 'HC01_EST_VC41', 'HC01_EST_VC42', 'HC01_EST_VC43', 'HC01_EST_VC47', 'HC01_EST_VC48', 'HC01_EST_VC49', 'HC01_EST_VC50', 'HC01_EST_VC51', 'HC01_EST_VC68', 'HC01_EST_VC69', 'HC01_EST_VC74', 'HC01_EST_VC75'])
            df.columns = names = ['ID', 'Census Tract', 'Population', 'Age 1-4', 'Age 5-17', 'Age 18-24', 'Age 25-34', 'Age 35-44', 'Age 45-54', 'Age 55-64', 'Age 65-74', 'Age 75+', 'Male', 'Female', 'White', 'Black', 'Native American', 'Asian', 'Pacific Islander', 'Other',
                                  'Two or more', 'Native Born', 'Naturalized', 'No Citizen', 'Never Married', 'Married', 'Divorced', 'Widowed', 'No High School', 'High School', 'Some College', 'Bachelor', 'Graduate', 'Poverty Below 100', 'Poverty 100-149', 'Household Owner', 'Household Renter']
            df['Year'] = yr
            df = df.reindex(columns=['Year', 'ID', 'Census Tract', 'Population', 'Age 1-4', 'Age 5-17', 'Age 18-24', 'Age 25-34', 'Age 35-44', 'Age 45-54', 'Age 55-64', 'Age 65-74', 'Age 75+', 'Male', 'Female', 'White', 'Black', 'Native American', 'Asian', 'Pacific Islander', 'Other',
                                     'Two or more', 'Native Born', 'Naturalized', 'No Citizen', 'Never Married', 'Married', 'Divorced', 'Widowed', 'No High School', 'High School', 'Some College', 'Bachelor', 'Graduate', 'Poverty Below 100', 'Poverty 100-149', 'Household Owner', 'Household Renter'])
            dfs.append(df)
        elif yr == '2011' or yr == '2012':
            df = pd.read_csv(f, header=0, skiprows=[1], usecols=[
                             'GEO.id', 'GEO.display-label', 'HC01_EST_VC01', 'HC01_EST_VC03', 'HC01_EST_VC04', 'HC01_EST_VC05', 'HC01_EST_VC06', 'HC01_EST_VC07', 'HC01_EST_VC08', 'HC01_EST_VC09', 'HC01_EST_VC10', 'HC01_EST_VC11', 'HC01_EST_VC16', 'HC01_EST_VC17', 'HC01_EST_VC21', 'HC01_EST_VC22', 'HC01_EST_VC23', 'HC01_EST_VC24', 'HC01_EST_VC25', 'HC01_EST_VC26',
                             'HC01_EST_VC27', 'HC01_EST_VC33', 'HC01_EST_VC35', 'HC01_EST_VC36', 'HC01_EST_VC41', 'HC01_EST_VC42', 'HC01_EST_VC43', 'HC01_EST_VC44', 'HC01_EST_VC49', 'HC01_EST_VC50', 'HC01_EST_VC51', 'HC01_EST_VC52', 'HC01_EST_VC53', 'HC01_EST_VC72', 'HC01_EST_VC73', 'HC01_EST_VC79', 'HC01_EST_VC80'])
            df.columns = names = ['ID', 'Census Tract', 'Population', 'Age 1-4', 'Age 5-17', 'Age 18-24', 'Age 25-34', 'Age 35-44', 'Age 45-54', 'Age 55-64', 'Age 65-74', 'Age 75+', 'Male', 'Female', 'White', 'Black', 'Native American', 'Asian', 'Pacific Islander', 'Other',
                                  'Two or more', 'Native Born', 'Naturalized', 'No Citizen', 'Never Married', 'Married', 'Divorced', 'Widowed', 'No High School', 'High School', 'Some College', 'Bachelor', 'Graduate', 'Poverty Below 100', 'Poverty 100-149', 'Household Owner', 'Household Renter']
            df['Year'] = yr
            df = df.reindex(columns=['Year', 'ID', 'Census Tract', 'Population', 'Age 1-4', 'Age 5-17', 'Age 18-24', 'Age 25-34', 'Age 35-44', 'Age 45-54', 'Age 55-64', 'Age 65-74', 'Age 75+', 'Male', 'Female', 'White', 'Black', 'Native American', 'Asian', 'Pacific Islander', 'Other',
                                     'Two or more', 'Native Born', 'Naturalized', 'No Citizen', 'Never Married', 'Married', 'Divorced', 'Widowed', 'No High School', 'High School', 'Some College', 'Bachelor', 'Graduate', 'Poverty Below 100', 'Poverty 100-149', 'Household Owner', 'Household Renter'])
            dfs.append(df)

    df = pd.concat(dfs)
    df.reset_index(inplace=True)
    df.drop('index', axis=1, inplace=True)
    df['Census Tract'] = df['Census Tract'].str.rstrip(
        ', District of Columbia, District of Columbia')
    df.loc[91, 'Household Owner'] = df.loc[91, 'Population'] - df.loc[91, 'Household Renter']
    df.loc[270, 'Household Owner'] = df.loc[270, 'Population'] - df.loc[270, 'Household Renter']
    df.loc[449, 'Household Owner'] = df.loc[449, 'Population'] - df.loc[449, 'Household Renter']
    df.loc[628, 'Household Owner'] = df.loc[628, 'Population'] - df.loc[628, 'Household Renter']
    df.loc[807, 'Household Owner'] = df.loc[807, 'Population'] - df.loc[807, 'Household Renter']
    own_rent_ratio = df['Household Owner'].sum() / (df['Household Owner'].sum() +
                                                    df['Household Renter'].sum())
    for i in [1, 86, 180, 265, 359, 444, 538, 623, 717, 802]:
        df.loc[i, 'Household Owner'] = int(df.loc[i, 'Population'] * own_rent_ratio)
        df.loc[i, 'Household Renter'] = int(df.loc[i, 'Population'] * (1 - own_rent_ratio))

    return df


def group_sample_fill(df):
    """Upsample the period from annual to monthly for each cluster."""
    cols = ['Population', 'Age 1-4', 'Age 5-17', 'Age 18-24', 'Age 25-34', 'Age 35-44',
            'Age 45-54', 'Age 55-64', 'Age 65-74', 'Age 75+', 'Male', 'Female', 'White', 'Black', 'Native American', 'Asian',
            'Pacific Islander', 'Other', 'Two or more', 'Native Born', 'Naturalized', 'No Citizen', 'Never Married', 'Married',
            'Divorced', 'Widowed', 'No High School', 'High School', 'Some College', 'Bachelor', 'Graduate', 'Poverty Below 100',
            'Poverty 100-149', 'Household Owner', 'Household Renter']

    df = df.groupby(['Cluster', 'Year'])[cols].sum()

    clusters = []
    for i in range(1, 46):
        cl_name = 'Cluster ' + str(i)
        cluster = df.loc[cl_name]
        cluster.index = pd.PeriodIndex(cluster.index, freq='A')
        cluster = cluster.resample('M').ffill()
        cluster['Cluster'] = cl_name
        clusters.append(cluster)

    df = pd.concat(clusters)
    df.reset_index(inplace=True)
    df.columns = ['Date', 'Population', 'Age 1-4', 'Age 5-17', 'Age 18-24', 'Age 25-34', 'Age 35-44',
                  'Age 45-54', 'Age 55-64', 'Age 65-74', 'Age 75+', 'Male', 'Female', 'White', 'Black', 'Native American', 'Asian',
                  'Pacific Islander', 'Other', 'Two or more', 'Native Born', 'Naturalized', 'No Citizen', 'Never Married', 'Married',
                  'Divorced', 'Widowed', 'No High School', 'High School', 'Some College', 'Bachelor', 'Graduate', 'Poverty Below 100',
                  'Poverty 100-149', 'Household Owner', 'Household Renter', 'Cluster']
    df = df.reindex(columns=['Date', 'Cluster', 'Population', 'Age 1-4', 'Age 5-17', 'Age 18-24', 'Age 25-34', 'Age 35-44',
                             'Age 45-54', 'Age 55-64', 'Age 65-74', 'Age 75+', 'Male', 'Female', 'White', 'Black', 'Native American', 'Asian',
                             'Pacific Islander', 'Other', 'Two or more', 'Native Born', 'Naturalized', 'No Citizen', 'Never Married', 'Married',
                             'Divorced', 'Widowed', 'No High School', 'High School', 'Some College', 'Bachelor', 'Graduate', 'Poverty Below 100',
                             'Poverty 100-149', 'Household Owner', 'Household Renter'])

    return df


def prep_df_ml():
    """Prepare data for machine learning."""
    df_char = create_char_table()

    df_cl = pg.saveTable('census_tracts')
    df_cl.columns = ['Census Tract', 'Lat', 'Lon', 'Cluster', 'Neighborhood']
    df = pd.merge(df_char, df_cl, on='Census Tract')

    df = group_sample_fill(df)

    return df


def prep_income_ml():
    """Prepare income data for machine learning."""
    df_inc = pg.saveTable(nameTable='mean_income')
    df_inc.columns = ['Year', 'Census Tract', 'Mean Income', 'Households', 'Total Income', 'Score']
    df_inc.drop('Score', axis=1, inplace=True)

    df_cl = pg.saveTable('census_tracts')
    df_cl.columns = ['Census Tract', 'Lat', 'Lon', 'Cluster', 'Neighborhood']
    df = pd.merge(df_inc, df_cl, on='Census Tract')

    df = group_sample_fill_income(df)

    df['Mean Income'] = (df['Total Income'] / df['Households']).round(0).astype(int)
    df.drop(['Households', 'Total Income'], axis=1, inplace=True)
    df.columns = ['Date', 'Cluster', 'Mean Income']
    df = df.reindex(columns=['Cluster', 'Date', 'Mean Income'])

    return df


def group_sample_fill_income(df):
    """Upsample the period from annual to monthly for each cluster."""
    df = df.groupby(['Cluster', 'Year'])['Households', 'Total Income'].sum()

    clusters = []
    for i in range(1, 46):
        cl_name = 'Cluster ' + str(i)
        cluster = df.loc[cl_name]
        cluster.index = pd.PeriodIndex(cluster.index, freq='A')
        cluster = cluster.resample('M').ffill()
        cluster['Cluster'] = cl_name
        clusters.append(cluster)

    df = pd.concat(clusters)
    df.reset_index(inplace=True)

    return df


def cluster_46_df():
    """Create missing cluster 46 data."""
    data = [['Cluster 46', '2011'],
            ['Cluster 46', '2012'],
            ['Cluster 46', '2013'],
            ['Cluster 46', '2014'],
            ['Cluster 46', '2015']]
    df = pd.DataFrame(data=data, columns=['Cluster', 'Date'])
    df.set_index('Date', inplace=True)
    df.index = pd.PeriodIndex(df.index, freq='A')
    df = df.resample('M').ffill()
    df.reset_index(inplace=True)
    df = df.reindex(columns=['Cluster', 'Date'])

    return df


def combine_all_cats():
    """Combine all the data into a final DataFrame ready for machine learning."""
    df_crime = pickle.load(open("data/crime.p", "rb"))
    df_crime.columns = ['Cluster', 'Date', 'Total Crimes', 'Day', 'Evening', 'Midnight', 'Arson', 'Assault', 'Burglary',
                        'Homicide', 'Motor Vehicle Theft', 'Robbery', 'Sex Abuse', 'Theft Auto', 'Theft Other', 'Gun', 'Knife', 'Others']

    df_char = prep_df_ml()
    df_inc = prep_income_ml()
    df_cl46 = cluster_46_df()

    df_char = df_char.append(df_cl46)
    df_char = df_char.reindex(columns=['Date', 'Cluster', 'Population', 'Age 1-4', 'Age 5-17', 'Age 18-24', 'Age 25-34', 'Age 35-44',
                                       'Age 45-54', 'Age 55-64', 'Age 65-74', 'Age 75+', 'Male', 'Female', 'White', 'Black', 'Native American', 'Asian',
                                       'Pacific Islander', 'Other', 'Two or more', 'Native Born', 'Naturalized', 'No Citizen', 'Never Married', 'Married',
                                       'Divorced', 'Widowed', 'No High School', 'High School', 'Some College', 'Bachelor', 'Graduate', 'Poverty Below 100',
                                       'Poverty 100-149', 'Household Owner', 'Household Renter'])
    df_char.reset_index(drop=True, inplace=True)
    df_char.fillna(0, inplace=True)
    df_char['Date'] = df_char['Date'].apply(str)
    df_inc = df_inc.append(df_cl46)
    df_inc.reset_index(drop=True, inplace=True)
    df_inc.fillna(0, inplace=True)
    df_inc['Date'] = df_inc['Date'].apply(str)

    df_rent = pd.read_csv('data/GrossRent_2015-2010_export 11-29-2017.csv')
    df_rent.columns = ['Date', 'Cluster', 'Median Rental Price']
    df_sales = pd.read_csv('data/Sale_price_Asked_2015-2010_export 11-29-2017.csv')
    df_sales.columns = ['Date', 'Cluster', 'Median Sales Price']

    df_int = pd.merge(df_char, df_inc)
    df_int2 = pd.merge(df_int, df_crime)
    df_int3 = pd.merge(df_int2, df_rent)
    df_int4 = pd.merge(df_int3, df_sales)

    df = df_int4
    df['Index'] = df['Date'] + ': ' + df['Cluster']
    df.set_index('Index', inplace=True)
    df.drop(['Date', 'Cluster'], axis=1, inplace=True)
    df.to_pickle('data/final_ml.p')


def max_display_df():
    """Maximize DataFrame display options."""
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 175)


def add_qscores(df):
    """Calculate a score for each instance which will be used to label clusters after machin learning."""
    years = ['2011', '2012', '2013', '2014', '2015']
    months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

    for year in years:
        for month in months:
            # Concat period string
            period = year + '-' + month

            # Create mask for period
            mask = df['Date'].str.match(period)
            # mask = df.index.get_level_values(0).str.match(period)
            mask_index = df[mask].index

            # Calculate and set difference from value to mean
            pop_mean = df[mask]['Population'].mean()
            pop_diff = df[mask]['Population'] - pop_mean
            pov_mean = df[mask]['Poverty Below 100'].mean()
            pov_diff = df[mask]['Poverty Below 100'] - pov_mean
            inc_mean = df[mask]['Mean Income'].mean()
            inc_diff = df[mask]['Mean Income'] - inc_mean
            home_mean = df[mask]['Median Price Asked'].mean()
            home_diff = df[mask]['Median Price Asked'] - home_mean
            crime_mean = df[mask]['Total Crimes'].mean()
            crime_diff = df[mask]['Total Crimes'] - crime_mean

            # Calculate and set Q-Scores
            q_pop = pd.qcut(pop_diff, 9, labels=[-4, -3, -2, -1, 0, 1, 2, 3, 4])
            df.loc[mask_index, 'Population Q-Score'] = q_pop
            q_pov = pd.qcut(pov_diff, 9, labels=[4, 3, 2, 1, 0, -1, -2, -3, -4])
            df.loc[mask_index, 'Poverty Q-Score'] = q_pov
            q_inc = pd.qcut(inc_diff, 9, labels=[-4, -3, -2, -1, 0, 1, 2, 3, 4])
            df.loc[mask_index, 'Income Q-Score'] = q_inc
            q_home = pd.qcut(home_diff, 9, labels=[-4, -3, -2, -1, 0, 1, 2, 3, 4])
            df.loc[mask_index, 'Home Q-Score'] = q_home
            q_crime = pd.qcut(crime_diff, 9, labels=[4, 3, 2, 1, 0, -1, -2, -3, -4])
            df.loc[mask_index, 'Crime Q-Score'] = q_crime

    df['Total Q-Score'] = df['Population Q-Score'] * .1 + df['Poverty Q-Score'] * .1 + \
        df['Crime Q-Score'] * .2 + df['Income Q-Score'] * .3 + df['Home Q-Score'] * .3

    return df

################################################################################
# Execution
################################################################################


if __name__ == '__main__':
    max_display_df()

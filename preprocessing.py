'''
Functions to Preprocess Data

Katy Koenig

May 2020
'''
import glob
from functools import reduce
import locale
import re
import pandas as pd

locale.setlocale(locale.LC_ALL, "")   


SPEC_CLEANING_FNS = {
    'income.csv': get_info_poverty,
    'population.csv': get_info_pop,
    'race.csv': get_race_info,
    'commute_time.csv': get_commute_info
}


def load_link_acs(folder='data/ACS/', cleaning_fns=SPEC_CLEANING_FNS):
    '''
    Reads in ACS data and links csvs

    Input:
        folder(str): name of folder in which data is located

    Output:
        df with all data linked by block group
    '''
    rel_files = glob.glob(f'{folder}/*.csv')
    csv_lst = [pd.read_csv(file) for file in rel_files]
    for (csv, cleaning_fn) in cleaning_fns.items()
    # total_acs = reduce(lambda x, y: pd.merge(x, y, on=['GEOID']), csv_lst)
    return rel_files


def get_info_pop(pop_df):
    '''
    '''
    pop_df['total_pop'] = pop_df['Total_population']
    working_age_cols = find_rel_cols(pop_df, 15, 65, '\d\d')
    pop_df['total_working_age'] = pop_df[working_age_cols].sum(axis=1)
    pop_df['pct_working_age'] = pop_df['total_working_age'] / pop_df['total_pop']
    return pop_df[['GEOID', 'total_pop', 'pct_working_age']]


def get_info_poverty(income_df):
    '''
    '''
    # poverty threshold for family of 4 is 26,200 (https://aspe.hhs.gov/poverty-guidelines)
    # closest bin starts at 25,000
    income_df['total_hh'] = income_df['Total_income']
    poverty_cols = find_rel_cols(income_df, 0, 25000, '\d.*?\d\,\d+')
    income_df['pct_hh_pov'] = income_df[poverty_cols].sum(axis=1) / income_df['Total_income']
    return income_df[['GEOID', 'total_hh', 'pct_hh_pov']]


def get_race_info(race_df):
    '''
    '''
    race_df['pct_white'] = race_df['White alone_race'] / race_df['Total_race']
    race_df['pct_black'] = race_df['Black or African American alone_race'] / race_df['Total_race']
    race_df['pct_other'] = 1.0 - (race_df['pct_white'] + race_df['pct_black'])
    return race_df[['GEOID', 'pct_white', 'pct_black', 'pct_other']]


def get_commute_info(commute_df):
    '''
    '''
    cols_to_show = ['GEOID', 'pct_long_commute']
    # Find percentage of "long commuters"
    # median commute overall was 30-34 mins so long commute anything above 35
    time_cols = ['Less than 10 minutes_commute_time',
                 '10 to 14 minutes_commute_time',
                 '15 to 19 minutes_commute_time',
                 '20 to 24 minutes_commute_time',
                 '25 to 29 minutes_commute_time',
                 '30 to 34 minutes_commute_time',
                 '35 to 44 minutes_commute_time',
                 '45 to 59 minutes_commute_time',
                 '60 or more minutes_commute_time']
    long_comm_cols = find_rel_cols(commute_df[time_cols], 35, 1000, '\d\d')
    commute_df['pct_long_commute'] = commute_df[long_comm_cols].sum(axis=1) / \
                                        commute_df['Total_commute_time']

    # Find pct of commuters by mode of transit
    mode_cols = [('Car, truck, or van_commute_time', 'pct_car'),
                 ('Walked_commute_time', 'pct_walk'),
                 ('Taxicab, motorcycle, bicycle, or other means_commute_time',
                    'pct_other'),
                 ('Public transportation (excluding taxicab)_commute_time',
                    'pct_transit')]
    for (mode_col, new_col_name) in mode_cols:
        commute_df[new_col_name] = commute_df[mode_col] / commute_df['Total_commute_time']
        cols_to_show.append(new_col_name)
    return commute_df[cols_to_show]


def get_employment_info(employ_df):
    '''
    '''
    # need to divide by total population (from another df)
    #employ_df['Total_employment'] / 



def find_rel_cols(df, min_val, max_val, regex_str):
    '''
    '''
    rel_cols_set = set()
    for col in df.columns:
        for num in re.findall(f'{regex_str}', col):
            if locale.atoi(num) >= min_val and locale.atoi(num) < max_val:
                rel_cols_set.add(col)
    return rel_cols_set


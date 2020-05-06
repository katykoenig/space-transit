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


def get_info_pop(pop_df):
    '''

    Input:
        pop_df

    Output:
    '''
    pop_df['total_pop'] = pop_df['Total_population']
    working_age_cols = find_rel_cols(pop_df, 15, 65, '\d\d')
    pop_df['total_working_age'] = pop_df[working_age_cols].sum(axis=1)
    pop_df['pct_working_age'] = pop_df['total_working_age'] / pop_df['total_pop']
    return pop_df[['GEOID', 'total_pop', 'pct_working_age']]


def get_info_poverty(income_df):
    '''
    # poverty threshold for family of 4 is 26,200
    # (https://aspe.hhs.gov/poverty-guidelines)
    # closest bin starts at 25,000

    Input:
        income_df:

    Output:
    '''
    income_df['total_hh'] = income_df['Total_income']
    poverty_cols = find_rel_cols(income_df, 0, 25000, '\d.*?\d\,\d+')
    income_df['pct_hh_pov'] = income_df[poverty_cols].sum(axis=1) / \
                              income_df['Total_income']
    return income_df[['GEOID', 'total_hh', 'pct_hh_pov']]


def get_race_info(race_df):
    '''
    Cleans up data re. race to show percentage breakdown

    Input:
        race_df: dataframe of basic race data

    Output: dataframe of pct pop that is white, black or another race
    '''
    race_df['pct_white'] = race_df['White alone_race'] / race_df['Total_race']
    race_df['pct_black'] = race_df['Black or African American alone_race'] / \
                           race_df['Total_race']
    race_df['pct_other'] = 1.0 - (race_df['pct_white'] + race_df['pct_black'])
    return race_df[['GEOID', 'pct_white', 'pct_black', 'pct_other']]


def get_commute_info(commute_df):
    '''
    Cleans up the data re. commuting to only show percentage of commuters
    with high commute times and percentage of commuters by commute mode.
    Note: as the median commute overall was 30-34 mins bin, a high/long commute
    was determined to be a commute time of 35+ mins

    Input:
        commute_df: dataframe of commuting info

    Output: df of pct of commuters with high commute time and by commute mode
    '''
    cols_to_show = ['GEOID', 'pct_long_commute']
    # Find percentage of "long commuters"
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
    Returns only relevant columns of employment data
    (Basically only so the loop in load_link_acs is clean)

    Input:
        employ_df: dataframe of employment info

    Output: a smaller version of the input df 
    '''
    return employ_df[['GEOID', 'Total_employment']]


def clean_employment(df):
    '''
    Gets the percent employed for each census block instead of
    total count of employed

    Input:
        df: full acs dataframe

    Output: full acs df with pct_employed col instead of a count of employment
    '''
    df['pct_employed'] = df['Total_employment'] / df['total_pop']
    return df.drop(columns=['Total_employment'])


def find_rel_cols(df, min_val, max_val, regex_str):
    '''

    Inputs:
        df:
        min_val(int/float):
        max_val(int/float):
        regex_str:

    Output
        rel_cols_set
    '''
    rel_cols_set = set()
    for col in df.columns:
        for num in re.findall(f'{regex_str}', col):
            if locale.atoi(num) >= min_val and locale.atoi(num) < max_val:
                rel_cols_set.add(col)
    return rel_cols_set


SPEC_CLEANING_FNS = {
    'income.csv': get_info_poverty,
    'population.csv': get_info_pop,
    'race.csv': get_race_info,
    'commute_time.csv': get_commute_info,
    'employment.csv': get_employment_info
}


def load_link_acs(folder='data/ACS/', cleaning_fns=SPEC_CLEANING_FNS):
    '''
    Reads in ACS data and links csvs

    Input:
        folder(str): name of folder in which data is located

    Output:
        df with all data linked by block group
    '''
    # Find related files
    rel_files = glob.glob(f'{folder}/*.csv')

    df_lst = []
    # Link with correct cleaning function
    for csvname in cleaning_fns.keys():
        saved_name = f'{folder}{csvname}'
        for file in rel_files:
            if file == saved_name:
                dirty_df = pd.read_csv(file)
                cleaned_df = cleaning_fns[csvname](dirty_df)
                df_lst.append(cleaned_df)
    # Merge all dfs together
    total_acs = reduce(lambda x, y: pd.merge(x, y, on=['GEOID']), df_lst)
    return clean_employment(total_acs)

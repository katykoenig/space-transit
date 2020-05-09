'''
Functions to Preprocess Data

Katy Koenig

May 2020
'''
import glob
import ast
from functools import reduce
import locale
import re
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import shape, Point

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
    race_df['pct_other_race'] = 1.0 - (race_df['pct_white'] + race_df['pct_black'])
    return race_df[['GEOID', 'pct_white', 'pct_black', 'pct_other_race']]


def get_commute_info(commute_df):
    '''
    Cleans up the data re. commuting to only show percentage of commuters
    with high commute times and percentage of commuters by commute mode.
    Note: as the median commute overall was 30-34 mins bin, a high/long commute
    was determined to be a commute time of 35+ mins
    top 1/3 of commuters 45 + mins

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
    long_comm_cols = find_rel_cols(commute_df[time_cols], 45, 1000, '\d\d')
    commute_df['pct_long_commute'] = commute_df[long_comm_cols].sum(axis=1) / \
                                        commute_df['Total_commute_time']

    # Find pct of commuters by mode of transit
    mode_cols = [('Car, truck, or van_commute_time', 'pct_car'),
                 ('Walked_commute_time', 'pct_walk'),
                 ('Taxicab, motorcycle, bicycle, or other means_commute_time',
                    'pct_other_transit_mode'),
                 ('Public transportation (excluding taxicab)_commute_time',
                    'pct_transit')]
    for (mode_col, new_col_name) in mode_cols:
        commute_df[new_col_name] = commute_df[mode_col] / \
                                   commute_df['Total_commute_time']
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


def get_vehicle_info(vehicle_df):
    '''
    Returns only relevant columns of vehicle data
    (Basically only so the loop in load_link_acs is clean)

    Input:
        vehicle_df: dataframe of vehicle ownership

    Output: a smaller version of the input df
    '''
    return vehicle_df[['GEOID', 'Total_num_vehicles']]


def find_pct_hisp(hisp_df):
    '''

    Input:
        hisp_df:

    Output:

    '''
    hisp_df['pct_hisp'] = hisp_df['Hispanic or Latino_hispanic_res'] / \
                          hisp_df['Total_hispanic_res']
    return hisp_df[['GEOID', 'pct_hisp']]


def find_per_pop(df, col1, col2, new_col):
    '''
    Gets the percent or per person count for each census block instead of
    total raw count

    Input:
        df: full acs dataframe
        col1(str): numerator column
        col2(str): denominator column (usually total pop or total households)
        new_col(str): new column name

    Output: full acs df with pct/per person count instead of a raw count
    '''
    
    df[new_col] = df[col1] / df[col2]
    return df.drop(columns=[col1])


def find_rel_cols(df, min_val, max_val, regex_str):
    '''
    Find relevant columns in a dataframe, i.e. those with numbers between the
    min and max values given

    Inputs:
        df: pandas df that has numbers in col name
        min_val(int/float): minimum value (inclusive)
        max_val(int/float): maximum value (exclusive) 
        regex_str: regex to extract numbers from col names

    Output
        rel_cols_set: set of relevant column names
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
    'employment.csv': get_employment_info,
    'num_vehicles.csv': get_vehicle_info,
    'hispanic_res.csv': find_pct_hisp
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
    for to_clean in [('Total_employment', 'total_pop', 'pct_employed'),
                     ('Total_num_vehicles', 'total_pop', 'veh_per_capita')]:
        col1, col2, new_col = to_clean
        total_acs = find_per_pop(total_acs, col1, col2, new_col)
    return total_acs


def load_blocks(filename='data/block_groups.shp'):
    '''

    Input:
        filename(str): name of shp/geojson file with block group data

    Output:
        blocks_gdf: gdf with block group ids and polygons
    '''
    # load in census blocks gpd
    blocks_gdf = gpd.read_file(filename)
    blocks_gdf = check_null_change_proj(blocks_gdf)
    buffer_len_meters = (0.5 * 1000) * 1.60934
    blocks_gdf['half_mile_rad'] = blocks_gdf.centroid.buffer(buffer_len_meters)
    blocks_gdf['area'] = blocks_gdf.area * 0.000621371
    blocks_gdf['GEOID'] = blocks_gdf['GEOID'].astype('int')
    return blocks_gdf


def check_null_change_proj(gdf):
    '''
    Drops any obs with lacking geometry specs and converts geometry
    to Mercator projection so we can use meters for a radius later

    Inputs: a gdf with 'geometry' col

    Output: a gdf with geometry col converted to Mercator projection
    '''
    return gdf[gdf.geometry.notnull()].to_crs(epsg=32618)


def load_bus_stops(bus_file='data/CTA Bus Stops.geojson'):
    '''
    Reads in bus stop data and cleans to show only regularly available stops
    and reconfigures the data so that each obs is a stop-line pair to account 
    for stops which have multiple bus lines running through them

    Input:
        bus_file(str): name of geojson file with bus stop data

    Output: cleaned gdf of bus stop data
    '''
    bus_gdf = gpd.read_file(bus_file)
    bus_gdf = check_null_change_proj(bus_gdf)
    bus_gdf = bus_gdf[(bus_gdf['routesstpg'].notnull()) & (bus_gdf['status'] == '1')]
    bus_gdf['num_routes'] = bus_gdf['routesstpg'].apply(lambda x: \
                                                  len(str.split(x, sep=',')))
    # replicate stops that have multiple buses lines
    full_bus_gdf = bus_gdf.loc[bus_gdf.index.repeat(bus_gdf.num_routes)] \
                            [['systemstop', 'public_nam', 'geometry']]
    return full_bus_gdf.rename(columns={'systemstop': 'stop_num',
                                        'public_nam': 'stop_name'})


def load_el_stops(l_file='data/CTA_-_System_Information_-_List_of__L__Stops.csv'):
    '''
    Reads in the L stop data and cleans it by turning it into a gdf
    Note: the default projection for socrata data (which the City of
    Chicago uses) is EPSG:4326 but this projection must be specified before
    converting it to Mercator projection

    Input:
        l_file(str): name of csv file with L stop data

    Output: gdf of L stop data, ready to play well with other data
    '''
    el_df = pd.read_csv(l_file)
    # convert to gpd
    el_df['Location']= el_df['Location'].apply(lambda x: ast.literal_eval(x))  
    el_df['geometry'] = el_df['Location'].apply(lambda x: Point(x[1], x[0]))  
    el_gdf = gpd.GeoDataFrame(el_df, geometry='geometry',
                             crs={'init': 'epsg:4326', 'no_defs': True})
    el_gdf = check_null_change_proj(el_gdf)[['STOP_ID', 'STOP_NAME', 'geometry']]
    return el_gdf.rename(columns={'STOP_ID': 'stop_num', 'STOP_NAME': 'stop_name'})


def join_count_stations(block_groups_gdf, stop_gdfs):
    '''
    Inputs:
        stop_gdfs: list of gdfs of stops (usually one for L and one for bus)
        block_groups_gdf: gdf with census block group info

    Output:
        block_groups_gdf: an updated gdf with number of transit stops
                          in a half mile radius of a census block group
    '''
    all_stops_gdfs = pd.concat(stop_gdfs)
    f = lambda x: np.sum(all_stops_gdfs.intersects(x))
    block_groups_gdf['num_stops'] = block_groups_gdf['half_mile_rad'].apply(f)
    return block_groups_gdf


def combine_all_data():
    '''
    '''
    acs_df = load_link_acs()
    blocks_gdf = load_blocks()
    el_stops_gdf = load_el_stops()
    bus_stops_gdf  = load_bus_stops()
    stops_blocks = join_count_stations(blocks_gdf, [el_stops_gdf, bus_stops_gdf])
    merged = pd.merge(stops_blocks, acs_df)
    merged['density'] = merged['total_pop'] / merged['area']
    merged['interaction'] = merged['num_stops'] * merged['pct_transit']
    return merged

# package imports
import textdistance
import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz

# python standard library
import os
import datetime as dt

user = 'bsmit'
root_path = f'C:/Users/{user}/Mathematica/HS COVID-19 Analytics - Documents/'

# closure_df = pd.read_csv("cleaned_files/COVID_district_clos_dates.csv")
closure_df = pd.read_csv(os.path.join(root_path, 'cleaned_files', 'COVID_districtlevel_close_dates.csv'))
instruction_df = pd.read_csv(os.path.join(root_path, 'cleaned_files', "COVID_district_instr_dates.csv"), encoding="latin_1")
ne_df = pd.read_csv(os.path.join(root_path, 'cleaned_files', 'NE_dates_leaid.csv'))

# CCD data
ccd_df_orig = pd.read_csv(os.path.join(root_path, 'cleaned_files', 'CCD', 'dist.dir18.csv'))
ccd_df_add_on = pd.read_csv(os.path.join(root_path, 'cleaned_files', 'CCD', 'dist.checkMatch.csv'))
ccd_df = ccd_df_orig.merge(ccd_df_add_on[['state_leaid', 'district_name']], how='inner', on='state_leaid')

# create dictionaries for mapping
# county-level static dates
static_county_endDates = pd.read_csv(os.path.join(root_path, 'cleaned_files', 'static_endDates', 'countywide_endDates.csv'))
static_county_endDates_dict = {k:v for k, v in zip(static_county_endDates.county_code, pd.to_datetime(static_county_endDates.endDate))}
static_county_closeDates = pd.read_csv(os.path.join(root_path, 'cleaned_files', 'static_closeDates', 'countywide_closeDates.csv'))
static_county_closeDates_dict = {k:v for k, v in zip(static_county_closeDates.county_code, pd.to_datetime(static_county_closeDates.real_closeDate))}

# state-level closure dates
state_closure_df = pd.read_excel(os.path.join(root_path, 'raw files', 'coronavirus-school-closures-data.xlsx'), header=1)
state_dict = {k:v for k,v in zip(state_closure_df['State Abbreviation'], state_closure_df['State Closure Start Date'])}
# state-level static endDates doesn't help
# static_state_endDates = pd.read_csv(os.path.join(root_path, 'cleaned_files', 'static_endDates', 'statewide_endDates.csv'))

# similarity_alg = fuzz.token_sort_ratio
similarity_alg = fuzz.token_set_ratio


###########################
# iterrate through closure dates to find matches in the instruction dates
for index, row in closure_df.iterrows():

    # limit search to within state
    instruction_df_sub = instruction_df.loc[instruction_df.state == row.state, :]
    
    if instruction_df_sub.shape[0] > 0:
        instruction_df_sub['comparison_district'] = row.district
        # apply the text distance algorithm
        instruction_df_sub['similarity'] = list(map(lambda x, y: similarity_alg(x, y), instruction_df_sub['comparison_district'], instruction_df_sub['district']))
        instruction_df_sub_clean = instruction_df_sub.sort_values('similarity', ascending=False).reset_index(drop=True)

        # gather values of interest
        match_similarity = instruction_df_sub_clean.loc[0, 'similarity']
        match_district = instruction_df_sub_clean.loc[0, 'district']
        match_end_date = instruction_df_sub_clean.loc[0, 'endDate']

        # 82 and up for similarity will be considered a match (I looked at the ~300 observations)
        if match_similarity >= 82:
            # transfer values of interest to dataframe
            closure_df.loc[index, 'similarity'] = match_similarity
            closure_df.loc[index, 'match_district'] = match_district
            closure_df.loc[index, 'endDate'] = match_end_date

df_clean = closure_df.drop('Unnamed: 0', axis=1)

# merge the rest of the scraped instruction data with the data from above
df_combined = instruction_df.merge(df_clean, how='left', left_on=['district', 'state'], right_on=['match_district', 'state'])
df_combined.rename(columns={'district_x': 'district', 'endDate_x': 'endDate'}, inplace=True)

# remove odd row
df_combined.loc[df_combined.endDate == 'Last Day for Students', 'endDate'] = np.nan

# sanity check
df_combined.loc[pd.to_datetime(df_combined.endDate) > dt.datetime(2020, 12, 30), 'endDate'] = np.nan

###########################
# Combine with the CCD file

# extract state from CCD for the merge
ccd_df['state'] = ccd_df.state_leaid.str.extract('([A-Z]{2}).+')

# try merge with the CCD using exact strings
# df_combined = df_out.merge(ccd_df, how = 'inner', left_on = ['district_x', 'state'], right_on = ['district_name', 'state'])

# iterrate through CCD file to find fuzzy matches in the combined dates file
for index, row in ccd_df.iterrows():

    # limit search to within state
    df_combined_sub = df_combined.loc[df_combined.state == row.state, :]

    # if the state has any matches
    if df_combined_sub.shape[0] > 0:
        df_combined_sub['comparison_district'] = row.district_name
        # apply the text distance algorithm
        df_combined_sub['ccd_similarity'] = list(map(lambda x, y: similarity_alg(x, y), df_combined_sub['comparison_district'], df_combined_sub['district']))
        df_combined_sub['y_ccd_similarity'] = list(map(lambda x, y: similarity_alg(x, y), df_combined_sub['comparison_district'], df_combined_sub['district_y']))
        df_combined_sub_clean = df_combined_sub.sort_values('ccd_similarity', ascending=False).reset_index(drop=True)

        # select the match
        match = df_combined_sub_clean.loc[0]

        # only fill in data for possible matches. below 50 indicates a poor match
        if match.ccd_similarity > 50:

            # transfer values of interest to dataframe
            ccd_df.loc[index, 'ccd_similarity'] = match.ccd_similarity
            ccd_df.loc[index, 'match_district'] = match.district
            ccd_df.loc[index, 'state'] = match.state
            ccd_df.loc[index, 'closeDate'] = match.closeDate
            ccd_df.loc[index, 'distanceDate'] = match.distanceDate
            ccd_df.loc[index, 'closeDate.source'] = match['closeDate.source']
            ccd_df.loc[index, 'closeDate.level'] = match['closeDate.level']
            ccd_df.loc[index, 'endDate'] = match.endDate
            ccd_df.loc[index, 'similarity'] = match.similarity

            # if there is an alternative match, then attach these values too
            if pd.notnull(match.match_district):
                df_combined_sub_clean_alt = df_combined_sub.sort_values('y_ccd_similarity', ascending=False).reset_index(drop=True)
                alt_match = df_combined_sub_clean_alt.loc[0]
                ccd_df.loc[index, 'orig_match'] = alt_match.district_y
                ccd_df.loc[index, 'y_ccd_similarity'] = alt_match.y_ccd_similarity


# DC charter schools
dc_charter_dict = {1100008: '06/19/2020', # Friendship PCS
                    1100019: '06/10/2020', # Washington Latin
                    1100030: '06/19/2020', # DC Public Schools
                    1100031: '06/12/2020', # KIPP DC PCS
                    1100048: '05/29/2020', # DC Prep PCS
                    1100051: '06/19/2020' # Hope PCS
                    }

def dc_dates(leaid):
    # fill in DC info - endDate: 24 Jun 2021 (subtract 1 year)
    if leaid in dc_charter_dict:
        # use the dates from the dictionary above
        return dc_charter_dict[leaid]
    else:
        return '06/24/2020'

ccd_df.loc[ccd_df.state == 'DC', 'endDate'] = ccd_df.loc[ccd_df.state == 'DC', 'leaid'].map(dc_dates)

# modify/fill NE info with the Nebraska read in file
for idx, row in ccd_df.loc[ccd_df.state == 'NE'].iterrows():
    n_matches = (row.state_leaid == ne_df.state_leaid).sum()
    if n_matches > 0:
        # grab the matching row from NE file
        matching_row = ne_df.loc[ne_df.state_leaid == row.state_leaid]
        closeDate, endDate = matching_row.closeDate.values[0], matching_row.endDate.values[0]
        ccd_df.loc[idx, 'closeDate'] = pd.to_datetime(closeDate)
        ccd_df.loc[idx, 'endDate'] = pd.to_datetime(endDate).date()

# fill in and format dates
ccd_df['real_closeDate'] = pd.to_datetime(ccd_df['closeDate'])

# fill in missing sources for closure date
ccd_df.loc[ccd_df["closeDate.source"].isnull(), 'closeDate.source'] = 'State-level file'

####################
"""
This next chunk is iterative so that we can incorporate manual judgements for district matches
"""

# read the manually edited file to scratch out any mismatches flagged in lea_district_linkage_for_manual_edit.xlsx
manual_df = pd.read_excel(os.path.join(root_path, 'cleaned_files', "lea_district_linkage_for_manual_edit.xlsx"))
manual_colnames = ['state_leaid', 'true_match', 'lev_similarity']

df_out = ccd_df.merge(manual_df[manual_colnames], how='left', on='state_leaid')

# remove data based on lev_similarity scores
for index, row in df_out.iterrows():
    if row.lev_similarity <= .33:
        df_out.loc[index, 'endDate'] = np.nan
        df_out.loc[index, 'distanceDate'] = np.nan
    elif row.lev_similarity > 0.33 and row.lev_similarity < 0.5:
        if pd.isnull(row.true_match):
            df_out.loc[index, 'endDate'] = np.nan
            df_out.loc[index, 'distanceDate'] = np.nan
        elif row.true_match == 0:
            df_out.loc[index, 'endDate'] = np.nan
            df_out.loc[index, 'distanceDate'] = np.nan

# group by district and state to eliminate duplicate matches
for match_dist, df in df_out.groupby(['match_district', 'state']):
    if df.shape[0] > 1:
        non_match_idx = df.iloc[1:].index
        df_out.loc[non_match_idx, 'endDate'] = np.nan
        df_out.loc[non_match_idx, 'distanceDate'] = np.nan

"""
# output a special file solely for the purpose of manually editing the matches
dist_alg = textdistance.levenshtein.normalized_similarity
df_out['lev_similarity'] = list(map(lambda x, y: dist_alg(x, y), df_out.district_ccd.fillna(''), df_out.match_district.fillna('')))
df_out.to_excel(os.path.join(root_path, 'cleaned_files', 'lea_district_linkage_v7_for_manual_edit.xlsx'), index=False)
"""

###### fill in missing dates
# make new endDate column taking the district close date if available, otherwise use the state closure date
df_out['state_closeDate'] = df_out.state.map(state_dict)
df_out['static_county_endDate'] = df_out.county_code.map(static_county_endDates_dict).dt.date
df_out['static_county_closeDate'] = df_out.county_code.map(static_county_closeDates_dict)

# use county-level data first to fill in missing dates (if available)
df_out.loc[df_out.endDate.isnull(), 'endDate'] = df_out['static_county_endDate']
df_out.loc[df_out.real_closeDate.isnull(), 'real_closeDate'] = df_out['static_county_closeDate']

# use state-level data second to fill in missing dates (if available)
df_out.loc[df_out.real_closeDate.isnull(), 'real_closeDate'] = df_out['state_closeDate']

# calculate the number of weekdays between real_closeDate and endDate
def date_diff(date1, date2):
    try:
        # np.busday_count does not count the end date, so add 1 to diff
        diff = np.busday_count(np.datetime64(date2.date()), np.datetime64(date1)) + 1
    except:
        if pd.notnull(date1):
            new_date1 = np.datetime64(dt.datetime.strptime(date1, '%m/%d/%Y').date())
            diff = np.busday_count(np.datetime64(date2.date()), new_date1) + 1
            return diff
        return np.nan
    return diff

for idx, row in df_out.iterrows():
    df_out.loc[idx, 'missed_days'] = date_diff(row.endDate, row.real_closeDate)

# if total_days_missed are negative, that means that school had already ended
# by the time schools were ordered to close. make them = 0
df_out.loc[df_out.missed_days < 0, 'missed_days'] = 0

df_out['Spring_Break'] = 5
# indicator if the endDate was after Memorial Day (May 25)
df_out['after_5/25'] = (pd.to_datetime(df_out.endDate) >= dt.date(2020, 5, 25)).astype(int)
# indicator if the endDate was after July 4
df_out['after_7/4'] = (pd.to_datetime(df_out.endDate) >= dt.date(2020, 7, 4)).astype(int)

# subtract indicators from missed days to account for Holidays
df_out['total_days_missed'] = df_out.missed_days - df_out.Spring_Break - df_out['after_5/25'] - df_out['after_7/4']

df_out.rename(columns={'district_name': 'district_ccd'}, inplace=True)

# ensure all the date columns are proper dtypes
date_cols = ['closeDate', 'static_county_closeDate', 'state_closeDate', 'real_closeDate', 'distanceDate', 'static_county_endDate', 'endDate']
df_out[date_cols] = df_out[date_cols].apply(pd.to_datetime)

# sanity check on dates
df_out.loc[df_out.endDate < dt.datetime(2020, 2, 1), 'endDate'] = np.nan

# if total_days_missed are negative, that means that school had already ended
# by the time schools were ordered to close. make them = 0
df_out.loc[df_out.total_days_missed < 0, 'total_days_missed'] = 0

# output analysis file
keep_cols = ['leaid', 'district_ccd', 'state_leaid', 'state', 'county_code', 'match_district', 'ccd_similarity', 
            'orig_match', 'similarity', 'closeDate.source', 'closeDate', 'static_county_closeDate', 'state_closeDate',
            'real_closeDate', 'distanceDate', 'static_county_endDate', 'endDate', 'missed_days', 'Spring_Break', 
            'after_5/25', 'after_7/4', 'total_days_missed']
df_final = df_out[keep_cols].sort_values(['state', 'district_ccd'])

df_final.to_excel(os.path.join(root_path, 'cleaned_files', 'lea_district_linkage_v8.xlsx'), index=False)


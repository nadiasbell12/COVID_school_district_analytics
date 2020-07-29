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
# remove state == 'BI' from CCD data
ccd_df = ccd_df.loc[ccd_df.state != 'BI']

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

# fill in missing sources for closure dates
ccd_df.loc[ccd_df["closeDate.source"].isnull(), 'closeDate.source'] = 'State-level file'

################
# This next chunk is iterative so that we can incorporate manual judgements for district matches

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

# another measure for comparing strings
dist_alg = textdistance.levenshtein.normalized_similarity
df_out['lev_similarity'] = list(map(lambda x, y: dist_alg(x, y), df_out.district_name.fillna(''), df_out.match_district.fillna('')))

# output a special file solely for the purpose of manually editing the matches
# df_out.to_excel(os.path.join(root_path, 'cleaned_files', 'lea_district_linkage_for_manual_edit_TEST.xlsx'), index=False)

# output a file with the variables of interest
keep_cols = ['leaid', 'district_name', 'state_leaid', 'state', 'county_code', 'match_district', 'ccd_similarity', 
            'orig_match', 'similarity', 'closeDate.source', 'closeDate', 'real_closeDate', 'distanceDate', 'endDate']
df_final = df_out[keep_cols].sort_values(['state', 'district_name'])

df_final.to_excel(os.path.join(root_path, 'cleaned_files', 'lea_district_linkage_v9.xlsx'), index=False)

# package imports
import pandas as pd
import numpy as np

# python standard library
import os, re
import datetime as dt

user = 'bsmit'
root_path = f'C:/Users/{user}/Mathematica/HS COVID-19 Analytics - Documents/'

df = pd.read_excel(os.path.join(root_path, 'cleaned_files', 'lea_district_linkage_v9.xlsx'))

# make dictionary for the enrollment per county
cnty_ccd_df = pd.read_csv(os.path.join(root_path, 'cleaned_files', 'CCD', 'cnty.ccd_dates.csv'))
cnty_enrol_dict = {k:v for k, v in zip(cnty_ccd_df.county_code, cnty_ccd_df.enr)}

# make dictionary for the enrollment per district
dist_ccd_df = pd.read_csv(os.path.join(root_path, 'cleaned_files', 'CCD', 'dist.ccd_dates.csv'))
dist_enrol_dict = {k:v for k, v in zip(dist_ccd_df.leaid, dist_ccd_df.enr)}

# attach enrollment figures
df['cnty_enrol'] = df.county_code.map(cnty_enrol_dict)
df['dist_enrol'] = df.leaid.map(dist_enrol_dict)

# construct weight based on enrollment - higher enrollment means higher weight for dates
df['dist_wgt'] = df.dist_enrol / df.cnty_enrol
# TODO: not sure how the district enrollment can be higher than the county enrollment (district covers multiple counties?)
# print(df[['cnty_enrol', 'dist_enrol', 'dist_wgt']].sort_values('dist_wgt', ascending=False))

###### fill in missing dates where the district or state has a constant date value

# create dictionaries for mapping
# county-level static dates
static_county_endDates = pd.read_csv(os.path.join(root_path, 'cleaned_files', 'static_endDates', 'countywide_endDates.csv'))
static_county_endDates_dict = {k:v for k, v in zip(static_county_endDates.county_code, pd.to_datetime(static_county_endDates.endDate))}
static_county_closeDates = pd.read_csv(os.path.join(root_path, 'cleaned_files', 'static_closeDates', 'countywide_closeDates.csv'))
static_county_closeDates_dict = {k:v for k, v in zip(static_county_closeDates.county_code, pd.to_datetime(static_county_closeDates.real_closeDate))}
# state-level closure dates
state_closure_df = pd.read_excel(os.path.join(root_path, 'raw files', 'coronavirus-school-closures-data.xlsx'), header=1)
state_closure_dict = {k:v for k,v in zip(state_closure_df['State Abbreviation'], state_closure_df['State Closure Start Date'])}
# state-level static endDates doesn't help
# static_state_endDates = pd.read_csv(os.path.join(root_path, 'cleaned_files', 'static_endDates', 'statewide_endDates.csv'))

# make new endDate column taking the district close date if available, otherwise use the state closure date
df['state_closeDate'] = df.state.map(state_closure_dict)
df['static_county_endDate'] = df.county_code.map(static_county_endDates_dict).dt.date
df['static_county_closeDate'] = df.county_code.map(static_county_closeDates_dict)

# use county-level data first to fill in missing dates (if available)
df.loc[df.endDate.isnull(), 'endDate'] = df['static_county_endDate']
df.loc[df.real_closeDate.isnull(), 'real_closeDate'] = df['static_county_closeDate']

# use state-level data second to fill in missing dates (if available)
df.loc[df.real_closeDate.isnull(), 'real_closeDate'] = df['state_closeDate']


############### Date imputation

# start counting from January 1, 2020
zero_date = pd.Timestamp(year=2020, month=1, day=1)
df.endDate = pd.to_datetime(df.endDate)

# construct variable counting number of days since the above given day
var_name_days_since = zero_date.strftime('days_since_%b_%d')
df[var_name_days_since] = df.endDate - zero_date

combined_df = pd.DataFrame()
# impute dates using county data (if available)
for cc, sub_df in df.groupby('county_code'):
    n_missing = sub_df.endDate.isnull().sum()
    # only need to impute if there are missing dates
    if n_missing > 0:
        # calculate the % missing
        n_rows = sub_df.shape[0]
        high_missing = n_missing/n_rows > .5

        # if all endDates in the county are missing
        if n_rows - n_missing == 0:
            in_state = sub_df.state.unique()[0]
            # if all endDates are missing, imputed_endDate will take on the average endDate in the state
            alt_df = df.loc[df.state.str.contains(in_state)]

            # In the case of DC, it will take on the average of neighboring states.
            if sub_df.state.unique()[0] == 'DC':
                # use the average of Maryland and Virginia
                alt_df = df.loc[df.state.str.contains('(MD|VA)')]

            # we can skip American Samoa, Guam, Puerto Rico, Virgin Islands
            elif re.search('AS|GU|PR|VI', sub_df.state.unique()[0], flags=re.I):
                continue
            
            mean_endDate_in_days = alt_df[var_name_days_since].dt.days.mean()

        elif n_rows - n_missing == 1:
            # if there's only one available endDate, use that one
            mean_endDate_in_days = sub_df[var_name_days_since].dt.days.mean()

        else:
            # calculate standard devitation of the dates
            sd = sub_df[var_name_days_since].dt.days.std()
            high_sd = sd > 5
            
            if high_missing or high_sd:
                # calculate the county's enrollment-adjusted mean endDate
                wgt_sum = sub_df.loc[sub_df[var_name_days_since].notnull(), 'dist_wgt'].sum() # missing dates not counted
                sub_df['district_wgt'] = sub_df.dist_wgt / wgt_sum # enter into dataframe
                weighted_days = sub_df[var_name_days_since].dt.days * (sub_df.dist_wgt / wgt_sum)
                mean_endDate_in_days = weighted_days.sum()
            else:
                # these dates are clustered nicely and few are missing, take the mode
                mode_endDate_in_days = sub_df[var_name_days_since].dt.days.mode()
                if len(mode_endDate_in_days.tolist()) == 1:
                    mean_endDate_in_days = sub_df[var_name_days_since].dt.days.mean()
                else:
                    # multiple modes, taking the weighted mean
                    wgt_sum = sub_df.loc[sub_df[var_name_days_since].notnull(), 'dist_wgt'].sum()
                    sub_df['district_wgt'] = sub_df.dist_wgt / wgt_sum # enter into dataframe
                    weighted_days = sub_df[var_name_days_since] * (sub_df.dist_wgt / wgt_sum)
                    mean_endDate_in_days = weighted_days.dt.days.sum()

        # change back into a date and add to dataframe
        imputed_endDate = zero_date + dt.timedelta(days = mean_endDate_in_days)
        sub_df.loc[sub_df.endDate.isnull(), 'imputed_endDate'] = imputed_endDate.date()
    
    combined_df = combined_df.append(sub_df, sort=False)

# remove 'BI' state from data
combined_df = combined_df.loc[combined_df.state != 'BI']

# calculate the number of weekdays between real_closeDate and endDate
def date_diff(date1, date2):
    # np.busday_count does not count the end date, so add 1 to diff
    diff = np.busday_count(np.datetime64(date2.date()), np.datetime64(date1.date())) + 1
    return diff

for idx, row in combined_df.iterrows():
    # use endDate if present; otherwise use imputed_endDate
    if pd.notnull(row.endDate):
        combined_df.loc[idx, 'missed_days'] = date_diff(row.endDate, row.real_closeDate)
    else:
        combined_df.loc[idx, 'missed_days'] = date_diff(pd.to_datetime(row.imputed_endDate), row.real_closeDate)

# if total_days_missed are negative, that means that school had already ended
# by the time schools were ordered to close. make them = 0
combined_df.loc[combined_df.missed_days < 0, 'missed_days'] = 0

combined_df['Spring_Break'] = 5
# indicator if the endDate was after Memorial Day (May 25)
combined_df['after_5/25'] = (pd.to_datetime(combined_df.endDate) >= dt.date(2020, 5, 25)).astype(int)
# indicator if the endDate was after July 4
combined_df['after_7/4'] = (pd.to_datetime(combined_df.endDate) >= dt.date(2020, 7, 4)).astype(int)

# subtract indicators from missed days to account for Holidays
combined_df['total_days_missed'] = combined_df.missed_days - combined_df.Spring_Break - combined_df['after_5/25'] - combined_df['after_7/4']

# ensure all the date columns are proper dtypes
date_cols = ['closeDate', 'static_county_closeDate', 'state_closeDate', 
            'real_closeDate', 'distanceDate', 'static_county_endDate', 'endDate']
combined_df[date_cols] = combined_df[date_cols].apply(pd.to_datetime)

# sanity check on dates
combined_df.loc[combined_df.endDate < dt.datetime(2020, 2, 1), 'endDate'] = np.nan

# if total_days_missed are negative, that means that school had already ended
# by the time schools were ordered to close. make them = 0
combined_df.loc[combined_df.total_days_missed < 0, 'total_days_missed'] = 0

df_out = combined_df.sort_values(['state', 'district_name'])

df_out.to_excel(os.path.join(root_path, 'cleaned_files', 'lea_district_linkage_imputations.xlsx'), index=False)

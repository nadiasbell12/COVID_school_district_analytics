# package imports
import pandas as pd
import numpy as np

# python standard library
import os, re
import datetime as dt

user = 'bsmit'
root_path = f'C:/Users/{user}/Mathematica/HS COVID-19 Analytics - Documents/'

df = pd.read_excel(os.path.join(root_path, 'cleaned_files', 'lea_district_linkage_v7.xlsx'))

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

# change dates to numeric - start counting from January 1, 2020
zero_date = pd.Timestamp(year=2020, month=1, day=1)

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

# iterate through to calculate missed days for rows with new endDates
for idx, row in combined_df.iterrows():
    if pd.isnull(row.endDate):
        diff = np.busday_count(row.real_closeDate.date(), row.imputed_endDate)
        combined_df.loc[idx, 'missed_days'] = diff
        # indicator if the imputed_endDate was after Memorial Day (May 25)
        combined_df.loc[idx, 'after_5/25'] = int(pd.to_datetime(row.imputed_endDate) >= dt.date(2020, 5, 25))
        # indicator if the imputed_endDate was after July 4
        combined_df.loc[idx, 'after_7/4'] = int(pd.to_datetime(row.imputed_endDate) >= dt.date(2020, 7, 4))
        # subtract indicators from missed days to account for Holidays
        combined_df.loc[idx, 'total_days_missed'] = diff - row.Spring_Break - row['after_5/25'] - row['after_7/4']
        combined_df.loc[idx, var_name_days_since] = (pd.Timestamp(row.imputed_endDate) - zero_date).days

combined_df.to_excel(os.path.join(root_path, 'cleaned_files', 'lea_district_linkage_imputations.xlsx'), index=False)


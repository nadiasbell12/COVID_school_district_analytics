# package imports
import pandas as pd
import numpy as np

# python standard library
import re
from datetime import date, datetime
import calendar

# This program reads in the CA school end dates given as a txt file.

# read in as text
with open('input\CA_cbedsora18b.txt', 'r', encoding="latin-1") as reader:
    all_text = reader.readlines()

col_names = all_text[0].strip('\n').split('\t')
col_names_clean = [re.search('[a-zA-Z]+', col_name).group(0) for col_name in col_names]

complete_dict = {}
acc = 0
for line in all_text:
    if re.search('End Date', line):
        dict_entry = {}
        line_split = line.strip('\n').split('\t')
        zipped = zip(col_names_clean, line_split)
        for col_name, value in zipped:
            dict_entry[col_name] = value
        complete_dict[acc] = dict_entry
        acc += 1

df = pd.DataFrame.from_dict(complete_dict, orient='index').set_index('SchoolName')

# 2019 endDates of each school
df['end_date'] = list(map(lambda x: datetime.strptime(x, '%Y%m%d'), df.Value))
df['end_year'] = df.end_date.dt.year
df['end_month'] = df.end_date.dt.month
df['end_week'] = df.end_date.dt.week
df['end_weekday'] = df.end_date.dt.weekday
df['end_day'] = df.end_date.dt.day
df['end_weekday_name'] = df.end_date.dt.day_name()

# get week of month for 2019
cal = calendar.Calendar()

# obtain which week if the given month
def get_week_of_month(year, month, day, dayofweek):
    month_breakdown = cal.monthdayscalendar(2019, month)
    week_of_month = 0
    for week in month_breakdown:
        if week[dayofweek] == 0:
            continue
        if day not in week:
            week_of_month += 1
        else:
            return week_of_month

def infer_date(month, weekofmonth, dayofweek):
    month_breakdown = cal.monthdayscalendar(2020, month)
    week_of_month = 0
    for week in month_breakdown:
        if week[dayofweek] == 0:
            continue
        elif week_of_month != weekofmonth:
            week_of_month += 1
        else:
            return date(2020, month, week[dayofweek])
    return date(2020, month, month_breakdown[weekofmonth - 1][dayofweek])

for idx, row in df.iterrows():
    weekofmonth = get_week_of_month(row.end_year, row.end_month, row.end_day, row.end_weekday)
    df.loc[idx, 'weekofmonth'] = weekofmonth
    # infer date for 2020
    new_date = infer_date(row.end_month, weekofmonth, row.end_weekday)
    df.loc[idx, 'infered_endDate'] = new_date

df.to_excel('output/California_inferred_School_Dates.xlsx')

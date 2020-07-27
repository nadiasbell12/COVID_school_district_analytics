# package imports
import pandas as pd

# python standard library
import re

# read in as text
with open('input\CA_districts.js', 'r') as reader:
    all_text = reader.readlines()

complete_dict = {}
acc = 0
for line in all_text:
    line_split = line.strip().split(' : ')
    line_clean = [item.strip('[", ]') for item in line_split]
    if len(line_clean) > 1:
        if re.search('GEOID', line):
            # start a new dictionary
            dict_entry = {}
        dict_entry[line_clean[0]] = line_clean[1]
        if re.search('Link', line):
            # add to larger dictionary
            complete_dict[acc] = dict_entry
            acc += 1
df_out = pd.DataFrame.from_dict(complete_dict, orient='index').set_index('District')

df_out.to_csv('output/CA_close_dates.csv')

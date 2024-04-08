# This is sample code lib for generating sample data with Pandas
import pandas as pd
import random

# variables for sample
input_data = "/usr/local/input.csv"
output_file = "/usr/local/output.csv"
appendcolumn1 = "new column name 1"
appendcolumn2 = "new column name 2"
appendcolumn3 = "new column name 3"
# num values from 1 to 10
appendvalues0 = list(range(1,11))
appendvalues0_weight = [10,11,12,13,13,13,14,15,16,17]

# import data from source file
df_origin = pd.read_csv(input_data)

# add columns to dataframe having sample records from input source file
df_sample = df_origin.sample(n=10000)
df_sample[appendcolumn1] = "sample"
df_sample[appendcolumn2] = 0
df_sample[appendcolumn3] = ""
appendcolumn3_location = df_sample.columns.get_loc(appendcolumns3)

# set values on added columns
for i in range(len(df_sample)):
    # set sample values with weight
    appendvalues = random.choices(appendvalues0,k=1,weights=appendvalues0_weight)
    df_sample.iloc(i,appendcolumn3_locaiton) = appendvalues

df_sample.to_csv(output_file, index=False)

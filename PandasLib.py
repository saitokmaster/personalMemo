# Pandas sample code libs.
import pandas as pd

# variables for sample
local_file1 = "/usr/local/file1.csv"
local_file2 = "/usr/local/file2.csv"
output_file1 = "/usr/local/outputfile1.csv"
target_column_a = "column name A"
new_column_a = "new column name A"
output_columnname=[target_column_a,new_column_a]

# importing csv into dataframe
dataframe1 = pd.read(local_file1)
dataframe2 = pd.read(local_file2)

# filtering a column with dataframe
dataframe1_a = dataframe1.filter(target_column_a)
dataframe2_a = dataframe2.filter(target_column_a)

# merging dataframe1 and dataframe2
dataframe1_2 = pd.concat([dataframe1_a,dataframe2_a])

# dropping dupe values in "column name A"
dataframe1_2.drop_duplicates(subset=target_column_a,inplace=True)

# appending new column (0 as default)
dataframe1_2[new_column_a] = 0

# Output processed dataframe records as csv with a column filter
dataframe1_2.to_csv(output_file1,columns=output_columnnames,index=False)

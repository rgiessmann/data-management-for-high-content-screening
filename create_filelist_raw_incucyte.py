import os
import sys

import pandas

os.makedirs("./filelists_raw/", exist_ok=True)

ROOT_FOLDERNAME_WITHIN_SUBMISSION_SYSTEM = ""

#FILENAME = "20210920 Overview CG plates and compounds.xlsx"
#FILENAME = "20220121 Overview CG plates and compounds  _consolidated RTG.xlsx"
FILENAME = sys.argv[1]

df_imagings = pandas.read_excel(FILENAME, sheet_name="imaging campaigns")
## do only CQ1
df_imagings = df_imagings[df_imagings["experiment ID"].str.contains("cv")]

##
df_collector = df_imagings.copy()

## append all metadata columns but specified ones to all entries
exclude_columns= [  "imaging ID",
                    "raw data available in zip file",
                    "processed images available in folder",
                    "cq1 analysis available in folder",
                    "incucyte analyzed data available in csv file",
                    "incucyte timestamp",
                    "timepoint in hours"
                    ]

for col in df_collector.columns:
    if col in exclude_columns:
        df_collector.drop(col, axis=1, inplace=True)

df_collector["Files"]  =  df_imagings["raw data available in zip file"]

df_collector.drop_duplicates(inplace=True)

## first column has to be "Files"
columns = list(df_collector.columns)
columns.remove("Files")
columns.insert(0, "Files")
df_collector = df_collector[columns]

print("writing the big filelist...")
df_collector.to_csv(  f"./filelists_raw/filelist_raw_data_incucyte.csv",  index=False)
df_collector.to_excel(f"./filelists_raw/filelist_raw_data_incucyte.xlsx", index=False)

print("done.")

import os
import sys

import pandas

os.makedirs("./filelists_cq1/", exist_ok=True)

ROOT_FOLDERNAME_WITHIN_SUBMISSION_SYSTEM = ""

#FILENAME = "20210920 Overview CG plates and compounds.xlsx"
#FILENAME = "20220121 Overview CG plates and compounds  _consolidated RTG.xlsx"
FILENAME = sys.argv[1]

df_batches = pandas.read_excel(FILENAME, sheet_name="compound batches")
df_compounds = pandas.read_excel(FILENAME, sheet_name="compounds")
df_identifier = df_batches.merge(df_compounds, on="compound ID", how="left", validate="m:1")

df_experiments = pandas.read_excel(FILENAME, sheet_name="experiments")
## do only CQ1
df_experiments = df_experiments[df_experiments["experiment ID"].str.contains("CQ1-ctf")]


## store expanded compound maps
print("expanding the compound maps...")
compound_map_dict = {}
for see, _ in df_experiments.groupby("compound map see corresponding excel table"):

    print(f"Checking the compound map '{see}'...")

    df_compound_map = pandas.read_excel(FILENAME, sheet_name=f"compound map {see}")

    ## expand with lookup-ed ID
    for i, s in df_compound_map.iterrows():
        #print(i)
        #print(s.compound_name)

        column_name_for_identification = "compound batch ID"

        if pandas.isna(s[column_name_for_identification]):
            continue

        result = df_identifier.query("`compound batch ID` == '{compound_name}'".format(compound_name= s[column_name_for_identification]))
        if type(s[column_name_for_identification]) == int:
            result = df_identifier.query("`compound batch ID` == {compound_name}".format(compound_name= s[column_name_for_identification]))
        #print(result)
        #assert len(result) == 1, (s, result)
        if len(result) == 1:
            #print(dff.loc[i])
            for col in result.columns:
                df_compound_map.loc[i, col]        = result.squeeze()[col]
        else:
            print("ERROR: couldn't lookup the compound name '{compound_name}'".format(compound_name= s[column_name_for_identification]))

    compound_map_dict.update( {see: df_compound_map})



df_imagings = pandas.read_excel(FILENAME, sheet_name="imaging campaigns")
df_exclude = pandas.read_excel(FILENAME, sheet_name="exclude from file list")
## do only CQ1
df_imagings = df_imagings[df_imagings["experiment ID"].str.contains("CQ1-ctf")]

df_imagings = df_imagings.merge(df_experiments, on="experiment ID")

df_collector_all = []
for groupname, groupentries in df_imagings.groupby("experiment ID"):
    print(groupname)

    print("processing the imagings...")
    df_collector_one_experiment = []
    for i, s in groupentries.iterrows():
        assert not pandas.isna(s["processed images available in folder"])

        see = s["compound map see corresponding excel table"]
        df_compound_map = compound_map_dict[see].copy()

        ## append all metadata columns but specified ones to all entries
        exclude_columns= [  "compound map see corresponding excel table",
                            "imaged in instrument",
                            "raw data available in zip file",
                            "processed images available in folder",
                            "csv files available in folder",
                            ]
        for col in s.index:
            if not col in exclude_columns:
                df_compound_map.loc[:, col ]       = s[col]


        for x, y in df_compound_map.iterrows():

            ## check for exclusion
            if s["imaging campaign ID"] in df_exclude["imaging campaign ID"].values:
                if y["well ID"] in df_exclude[ df_exclude["imaging campaign ID"] == s["imaging campaign ID"] ]["well ID"].values:
                    ## exclude this well from being written to file list -- actual dropping is done later
                    print(f'{s["imaging campaign ID"]}: excluding well {y["well ID"]}')
                    df_compound_map.loc[x, "Files"] = None
                    continue

            ## construct well name, e.g. B-02 ; take care of padding single-digit numbers!
            well_name = ""
            well_name += y["well name"][0]
            well_name += "-"
            if len(y["well name"][1:]) == 1:
                well_name += "0"
            well_name += y["well name"][1:]

            df_compound_map.loc[x, "Files"]  =   ROOT_FOLDERNAME_WITHIN_SUBMISSION_SYSTEM +      \
                                        s["processed images available in folder"] +              \
                                        well_name + "_"+      \
                                        "F0001_T0001_Z0001.png"

        ## drop excluded entries
        df_compound_map = df_compound_map.dropna(subset=["Files"])

        ## first column has to be "Files"
        columns = list(df_compound_map.columns)
        columns.remove("Files")
        columns.insert(0, "Files")
        df_compound_map = df_compound_map[columns]

        df_collector_all.append(df_compound_map)
        df_collector_one_experiment.append(df_compound_map)

    df_output_one_experiment = pandas.concat(df_collector_one_experiment)
    df_output_one_experiment.to_csv(  f"./filelists_cq1/filelist_cq1_processed_images_experiment_id_{groupname}.csv",  index=False)
    df_output_one_experiment.to_excel(f"./filelists_cq1/filelist_cq1_processed_images_experiment_id_{groupname}.xlsx", index=False)


df_output_all_experiments = pandas.concat(df_collector_all)

print("writing the big filelist...")
df_output_all_experiments.to_csv(  f"./filelists_cq1/filelist_cq1_processed_images_all_experiments.csv",  index=False)
df_output_all_experiments.to_excel(f"./filelists_cq1/filelist_cq1_processed_images_all_experiments.xlsx", index=False)

print("done.")

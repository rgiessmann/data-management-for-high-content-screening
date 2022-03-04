import glob
import os
import sys

import numpy
import pandas

#METADATA_FILENAME = "20220121 Overview CG plates and compounds _small.xlsx"
METADATA_FILENAME = sys.argv[1]

## if you want to evaluate only a single imaging, do like this:
# QUERY = "`imaging campaign ID` == 'CQ1-ctf004-t12'"
## if you want to select all imagings, use:
QUERY = "ilevel_0 in ilevel_0"
## if you want to do selected imagings, use:
#QUERY = "`imaging campaign ID` == 'CQ1-ctf004-t0' or `imaging campaign ID` == 'CQ1-ctf004-t12'"


DO_I_HAVE_TO_MERGE_FILES_FIRST = True


def gather_csv_data_into_one_file(path_to_csv_files, output_filename = "output"):
    print(f"merging csv files in {path_to_csv_files} ...")
    filenames = glob.glob(f"{path_to_csv_files}/*Stats*.csv")
    filenames = list([os.path.basename(f) for f in filenames])
    if len(filenames)==0:
        print("ERROR: no csv files found in the indicated location!")
        raise

    keys_of_files = [i[:-4] for i in filenames]

    ## check for titles longer than 31 characters -- some applications may not be able to read the file
    keys_of_files_shortened = list(key[:31] for key in keys_of_files)
    if len(set(keys_of_files_shortened)) < len(keys_of_files):
        raise Exception


    df_collect_all = None
    for i, (filename_basename, filename_shortened) in enumerate(zip(keys_of_files, keys_of_files_shortened), start=1):
        filename = filename_basename + ".csv"

        print(f"Acting on file {i} of {len(keys_of_files)} ({filename})...")

        df = pandas.read_csv(os.path.join(path_to_csv_files, filename))

        RECOGNIZE_RELEVANT_COLUMN_WITH_THIS_STRING = '] Count'
        column_names_which_contain_the_word_count = [col for col in df.columns if
                                                    RECOGNIZE_RELEVANT_COLUMN_WITH_THIS_STRING in col]
        assert len(column_names_which_contain_the_word_count) == 1

        #print(column_names_which_contain_the_word_count)

        WHAT_TO_PUT_IN_FRONT_OF_NEW_NAME_OF_RELEVANT_COLUMN = "Cell_Count_"
        new_name_of_relevant_column = f"{WHAT_TO_PUT_IN_FRONT_OF_NEW_NAME_OF_RELEVANT_COLUMN}{filename_shortened}"
        df_renamed = df.rename(columns={ column_names_which_contain_the_word_count[0]: new_name_of_relevant_column })
        #print(df_renamed)

        MERGE_IF_THOSE_COLUMNS_ARE_EXACT_MATCHES = [
            # "ID" is not the same in all files...
            "WellID",
            "Row",
            "Column",
            "RowName",
            "ColumnName",
            "WellName",
            "DateTime",
            "Timepoint",
            "ElapsedTime",
            "Description",
        ]

        KEEP_THOSE_COLUMNS_INITIALLY = [
            # "ID" is not the same in all files...
            "WellID",
            "Row",
            "Column",
            "RowName",
            "ColumnName",
            "WellName",
            "DateTime",
            "Timepoint",
            "ElapsedTime",
            "Description"
        ]

        if df_collect_all is None:
            df_collect_all = df_renamed[KEEP_THOSE_COLUMNS_INITIALLY]
            df_collect_all["well name"] = df_renamed["WellName"].str.replace("-","")


        for col in MERGE_IF_THOSE_COLUMNS_ARE_EXACT_MATCHES:
            for x, y in zip(df_collect_all[col].values, df_renamed[col].values):
                if pandas.isna(x) and pandas.isna(y):
                    continue
                assert x == y, f"I expected that all tables would have the exactly same structure, but this is not the case: '{x}' != '{y}' "

        assert not new_name_of_relevant_column in df_collect_all.columns
        df_collect_all[new_name_of_relevant_column] = df_renamed[new_name_of_relevant_column]


    print("Writing the file...")
    df_collect_all.to_excel(output_filename, index=False)
    print("...done.")

    return df_collect_all





#### --- GET THE COMPOUND IDENTIFIERS ---
df_batches = pandas.read_excel(METADATA_FILENAME, sheet_name="compound batches")
df_compounds = pandas.read_excel(METADATA_FILENAME, sheet_name="compounds")
df_identifier = df_batches.merge(df_compounds, how="left", on="compound ID", validate="m:1")


## store expanded compound maps
print("expanding the compound maps...")
df_experiments = pandas.read_excel(METADATA_FILENAME, sheet_name="experiments")
compound_map_dict = {}
for see, _ in df_experiments.groupby("compound map see corresponding excel table"):
    print(f"Expanding compound map '{see}'...")

    df_compound_map = pandas.read_excel(METADATA_FILENAME, sheet_name=f"compound map {see}")
    df_compound_map = df_compound_map.merge(df_identifier, how="left", on="compound batch ID", validate="m:1")
    #print(df_compound_map)

    compound_map_dict.update( {see: df_compound_map})


####

df_imagings = pandas.read_excel(METADATA_FILENAME, sheet_name="imaging campaigns")

## select only a subset of imagings to evaluate...
df_imagings = df_imagings.query(QUERY)


df_imagings = df_imagings.merge(df_experiments, how="left", on="experiment ID")

df_collector = []
for groupname, groupentries in df_imagings.groupby("experiment ID"):
    print(f"processing the imagings for {groupname}...")
    for i, s in groupentries.iterrows():
        see = s["compound map see corresponding excel table"]
        df_compound_map = compound_map_dict[see].copy()

        ## append all metadata columns but specified ones to all entries
        exclude_columns= [  "compound map see corresponding excel table",
                            "imaged in instrument",
                            "raw data available in zip file",
                            "processed images available in folder",
                            "cq1 analysis available in folder",
                            ]
        for col in s.index:
            if not col in exclude_columns:
                df_compound_map.loc[:, col ]       = s[col]

        assert not pandas.isna(s["cq1 analysis available in folder"])

        merged_filename = os.path.join(s["cq1 analysis available in folder"], "merged.xlsx")

        if DO_I_HAVE_TO_MERGE_FILES_FIRST:
            where_are_the_csv_files = os.path.join(s["cq1 analysis available in folder"], "Reports")
            gather_csv_data_into_one_file(where_are_the_csv_files, merged_filename)

        df = pandas.read_excel(merged_filename)
        df = df.merge(df_compound_map, how="left", left_on="WellID", right_on="well ID")

        df_rows_which_are_control_wells = df[ df["experimental type"] == "blank" ]
        mean_values_for_control_rows = cf = df_rows_which_are_control_wells.mean()

        for x, y in [["calc", df], ["ctrl", cf]] :
            df[f"{x}1a"] = y["Cell_Count_Cell_Stats_HighIntObj"]          / y["Cell_Count_Cell_Stats"]
            df[f"{x}1b"] = y["Cell_Count_Cell_Stats_Normal"]              / y["Cell_Count_Cell_Stats"]

            df[f"{x}2a"] = y["Cell_Count_Cell_Stats_HealthyNuc"]          / y["Cell_Count_Cell_Stats_Normal"]
            df[f"{x}2b"] = y["Cell_Count_Cell_Stats_FragNuc"]             / y["Cell_Count_Cell_Stats_Normal"]
            df[f"{x}2c"] = y["Cell_Count_Cell_Stats_PyknoNuc"]            / y["Cell_Count_Cell_Stats_Normal"]

            df[f"{x}3a"] = y["Cell_Count_Cell_Stats_mitosis"]             / y["Cell_Count_Cell_Stats_PyknoNuc"]
            df[f"{x}3b"] = y["Cell_Count_Cell_Stats_apoptosis"]           / y["Cell_Count_Cell_Stats_PyknoNuc"]

            df[f"{x}4a"] = y["Cell_Count_Cell_Stats_membrane intact"]     / y["Cell_Count_Cell_Stats_HealthyNuc"]
            df[f"{x}4b"] = y["Cell_Count_Cell_Stats_membrane permeab"]    / y["Cell_Count_Cell_Stats_HealthyNuc"]

            df[f"{x}5a"] = y["Cell_Count_Cell_Stats_mito mass high"]      / y["Cell_Count_Cell_Stats_HealthyNuc"]
            df[f"{x}5b"] = y["Cell_Count_Cell_Stats_mito mass normal"]    / y["Cell_Count_Cell_Stats_HealthyNuc"]

            df[f"{x}6a"] = y["Cell_Count_Cell_Stats_tubulin effect"]      / y["Cell_Count_Cell_Stats_HealthyNuc"]
            df[f"{x}6b"] = y["Cell_Count_Cell_Stats_tubulin normal"]      / y["Cell_Count_Cell_Stats_HealthyNuc"]

            df[f"{x}_count_healthy"]        = y["Cell_Count_Cell_Stats_Normal"]
            df[f"{x}_count_all"]            = y["Cell_Count_Cell_Stats"]

        df["ratio_count_all"]       = df["calc_count_all"]      / df["ctrl_count_all"]
        df["ratio_count_healthy"]   = df["calc_count_healthy"]  / df["ctrl_count_healthy"]

        df.to_excel(merged_filename + "_evaluated.xlsx", index=False)

        df_collector.append(df)

print("Merging all results...")
df = pandas.concat(df_collector, ignore_index=True)
#df.reset_index(drop=True, inplace=True)


print("Trying to calculate relative growth...")
## pre-create new columns
df["relative_growth"] = None
df["mean_cell_count_blank_at_current_timepoint"]     = None
df["mean_cell_count_blank_at_t0"]     = None
df["cell_count_at_t0"]     = None
for groupname, groupentries in df.groupby("experiment ID"):
    t0_imaging = df_imagings[df_imagings["timepoint in hours"]==0][df_imagings["experiment ID"]==groupname]
    if len(t0_imaging) != 1:
        print(f"ERROR: Could not identify t0 for experiment {groupname}. Leaving relative growth empty.")
    else:
        t0_df = groupentries[ groupentries["timepoint in hours"]==0 ]
        mean_cell_count_blank_at_t0 = t0_mean_control = t0_df[ t0_df["experimental type"] == "blank" ][["Cell_Count_Cell_Stats_Normal"]].mean()
        cell_count_at_t0 = t0_all_values   = t0_df[["WellID", "Cell_Count_Cell_Stats_Normal"]].rename({"Cell_Count_Cell_Stats_Normal":"cell_count_at_t0"}, axis="columns")


        for timepoint, timepointentries in groupentries.groupby("timepoint in hours"):

            mean_cell_count_blank_at_current_timepoint = timepointentries[ timepointentries["experimental type"] == "blank"][["Cell_Count_Cell_Stats_Normal"]].mean()
            cell_count_at_current_timepoint = timepointentries[["WellID", "Cell_Count_Cell_Stats_Normal"]].rename({"Cell_Count_Cell_Stats_Normal":"cell_count_at_current_timepoint"}, axis="columns")
            this = cell_count_at_current_timepoint

            this["index"] = this.index
            this = this.merge(cell_count_at_t0, on="WellID", how="left")
            this.index = this["index"]
            this["divided"] = this["cell_count_at_current_timepoint"]/ this["cell_count_at_t0"]
            this["divided"] = this["divided"].apply(numpy.log2)
            y = float(numpy.log2(mean_cell_count_blank_at_current_timepoint /mean_cell_count_blank_at_t0))
            ## work around divided-by-zero error
            if y == 0:
                y = float("nan")

            def calculate(x):
                try:
                    return (2**((x/y)))-1
                except OverflowError:
                    print("WARNING: OverflowError encountered -- setting relative growth to None.")
                    return None
                except ZeroDivisionError:
                    print("WARNING: ZeroDivisionError encountered -- setting relative growth to None.")
                    return None

            this["relative_growth"] = this["divided"].map(calculate)
            ## actual formula is:
            #  relative_growth = (2**(log2(cell_count_at_current_timepoint/cell_count_at_t0)/log2(mean_cell_count_blank_at_current_timepoint/mean_cell_count_blank_at_t0)))-1
            print(this)

            df.iloc[ this.index, list(df.columns).index("mean_cell_count_blank_at_t0")] = [mean_cell_count_blank_at_t0]*len(this.index)
            df.iloc[ this.index, list(df.columns).index("mean_cell_count_blank_at_current_timepoint")] = [mean_cell_count_blank_at_current_timepoint]*len(this.index)
            df.iloc[ this.index, list(df.columns).index("cell_count_at_t0")] = this["cell_count_at_t0"]
            df.iloc[ this.index, list(df.columns).index("relative_growth")] = this["relative_growth"]


print("Writing all results into one file...")
df.to_excel("cq1_evaluated_all.xlsx", index=False)


print("done.")



#evaluate-incucate-raw-data.py
import sys

import numpy
import pandas

#METADATA_FILENAME = "20220121 Overview CG plates and compounds  _consolidated RTG.xlsx"
METADATA_FILENAME = sys.argv[1]
WHICH_EXPERIMENT = "cv001"

SEPARATOR_CHARACTER = ";"
DECIMAL_SEPARATOR   = ","

def quality_control(FILENAME_OF_RAW_DATA):
    ## quality control
    with open(FILENAME_OF_RAW_DATA) as f:
        assert f.readline().replace(SEPARATOR_CHARACTER,"") != "\n"
        assert f.readline().replace(SEPARATOR_CHARACTER,"") == "\n"
        assert f.readline().split(":")[0] == "Vessel Name"
        assert f.readline().split(":")[0] == "Metric"
        assert f.readline().split(":")[0] == "Cell Type"
        assert f.readline().split(":")[0] == "Passage"
        assert f.readline().split(":")[0] == "Notes"
        assert f.readline().split(":")[0] == "Analysis"
        assert f.readline().replace(SEPARATOR_CHARACTER,"") == "\n"
        assert f.readline().replace(SEPARATOR_CHARACTER,"").startswith("Date TimeElapsed")
        remaining_lines = f.readlines()


## read
compounds       = pandas.read_excel(METADATA_FILENAME, sheet_name="compounds")
batches         = pandas.read_excel(METADATA_FILENAME, sheet_name="compound batches")
df_identifier   = batches.merge(compounds, how="left", on="compound ID", validate="m:1")


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
df_imagings = df_imagings.merge(df_experiments, on="experiment ID")

## select only a subset of imagings to evaluate...
## comment out to act on all entries (or change to act on different subset)
df_imagings = df_imagings[df_imagings["experiment ID"] == WHICH_EXPERIMENT]


df_collector = []
for groupname, groupdf in df_imagings.groupby("experiment ID"):
    print(groupname)

    ## should be all the same
    assert groupdf["compound map see corresponding excel table"].nunique() == 1
    see = groupdf["compound map see corresponding excel table"].unique()[0]

    df_compound_map = compound_map_dict[see].copy()

    ## should be all the same
    assert groupdf["experiment ID"].nunique() == 1
    experimentid = groupdf["experiment ID"].unique()[0]
    s_this_experiment = df_experiments[ df_experiments["experiment ID"]==experimentid ].squeeze(axis=0)

    ## append all metadata columns but specified ones to all entries
    exclude_columns= [  "compound map see corresponding excel table",
                        "imaged in instrument",
                        "raw data available in zip file",
                        "processed images available in folder",
                        "cq1 analysis available in folder",
                        ]
    for col in s_this_experiment.index:
        if not col in exclude_columns:
            df_compound_map.loc[:, col ] = s_this_experiment[col]
    print(df_compound_map.columns)

    ## should be all the same
    assert groupdf["incucyte analyzed data available in csv file"].nunique() == 1
    FILENAME_OF_RAW_DATA = groupdf["incucyte analyzed data available in csv file"].unique()[0]
    name_of_file = pandas.read_csv(FILENAME_OF_RAW_DATA, sep=SEPARATOR_CHARACTER, decimal=DECIMAL_SEPARATOR, nrows=1, names=["name"], usecols=[0])
    metadata     = pandas.read_csv(FILENAME_OF_RAW_DATA, sep=SEPARATOR_CHARACTER, decimal=DECIMAL_SEPARATOR, skiprows=2, nrows=6, names=["meta"], usecols=[0])
    df           = pandas.read_csv(FILENAME_OF_RAW_DATA, sep=SEPARATOR_CHARACTER, decimal=DECIMAL_SEPARATOR, skiprows=9)

    timepoint_columns = list([x for x in range(len(df))])

    ## treat
    ## "second" measurement is actual t0; the point before was a "blank"
    df["Elapsed"] = df["Elapsed"] - df["Elapsed"].iloc[1]

    ## transpose for better access
    df = df.transpose()
    df_timings = df.loc[["Date Time", "Elapsed"],:]
    df = df.drop(index = ["Date Time", "Elapsed"])
    df["well name"] = df.index

    ## deeper quality control
    # catch value == 0, because this would yield invalid log2 transforms...
    problem = df[timepoint_columns].applymap(lambda x: True if x == 0 else False)
    if problem.any(axis="columns").any():
        print("---------------------------------")
        print("THERE IS A PROBLEM ('datapoint==0') IN YOUR DATA: ")
        print(df[problem.any(axis="columns")])
        print("---------------------------------")
        print("I am autofixing by replacing 0 with nan, but please investigate this datapoint!")
        print("This is the result of fixing:")
        indices_to_show_after_fixing = df[problem.any(axis="columns")].index
        for c in timepoint_columns:
            df[c] = df[c].replace({0: None})
        print(df.loc[indices_to_show_after_fixing])
        print("---")

    ## map compounds
    df = df.merge(df_compound_map, on="well name", how="left")


    ## create control = base line values
    selector = df["experimental type"] == "blank"
    control     = df[selector][timepoint_columns].mean()
    control_std = df[selector][timepoint_columns].std()


    ## calculate growth via Amelie's formula
    df_vs_blank = df[timepoint_columns].div(df[0], axis="index")
    df_vs_blank_log2 = df_vs_blank.applymap(numpy.log2)
    #print(df_vs_blank_log2)

    control_vs_blank = control.div(control[0])
    control_vs_blank_log2 = control_vs_blank.apply(numpy.log2)
    #print(control_vs_blank_log2)

    df_vs_blank_log2_div_control_vs_blank_log2 = df_vs_blank_log2.div(control_vs_blank_log2)
    df_vs_blank_log2_div_control_vs_blank_log2_derived = df_vs_blank_log2_div_control_vs_blank_log2.applymap(lambda x: 2**x-1)
    #print(df_vs_blank_log2_div_control_vs_blank_log2_derived)


    final = df_vs_blank_log2_div_control_vs_blank_log2_derived.copy()
    new_column_names = []
    new_column_names.append("growth_rate_between_blank_and_blank")
    new_column_names.append("growth_rate_between_t0_and_blank")
    english = ["first", "second", "third", "fourth", "fifth", "sixth", "seventh", "eighth"]
    for i in range(2,len(final.columns)):
        new_column_names.append(f"growth_rate_between_{english[i]}_timepoint_and_t0")
    final.columns = new_column_names
    final[ [c for c in df.columns if not c in timepoint_columns] ] = df[ [c for c in df.columns if not c in timepoint_columns] ]

    ## mark compounds with t4 < 0.5
    # final["timepoint4_smaller_than_05"] = final[4] < 0.5

    print(final)
    final.to_excel(FILENAME_OF_RAW_DATA + "_evaluated.xlsx", index=False)
    df_collector.append(final)

    ## groupby
    #for groupname, groupseries in final.groupby(["eubopen ID", "concentration"]):
    #    print(groupname)


    ## look into negative control performance (DMSO)
    # print(final[ final["compound name"]=="DMSO" ])
    # import matplotlib.pyplot as plt
    # for i, s in final[ final["compound name"]=="DMSO" ].iterrows():
    #     s[timepoint_columns].plot(label=s["well name"])
    # plt.legend()
    # plt.show()


    ## look into a specific compound by EUbOPEN ID
    # eubopen_id = "EUB0000502a"
    # print(final[ final["eubopen ID"]==eubopen_id ])
    # import matplotlib.pyplot as plt
    # for i, s in final[ final["eubopen ID"]==eubopen_id ].iterrows():
    #     s[timepoint_columns].plot(label=s["well name"])
    # plt.legend()
    # plt.show()

    ## look into all compounds
    # import matplotlib.pyplot as plt
    # for i, s in final.iterrows():
    #     s[timepoint_columns].plot() #label=s["well name"])
    # ax = plt.gca()
    # ax.set_ylim(0,2)
    # plt.show()

print("Writing all results into one file...")
pandas.concat(df_collector).to_excel("incucyte_evaluated_all.xlsx", index=False)

print("done.")

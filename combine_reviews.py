import os
import pandas as pd

# Find the files that we have saved
files = os.listdir("./reviews")

dfs = []

# Read them all and concatenate them into one dataframe
for file in files:
    cur_df = pd.read_csv("./reviews/" + file, sep="\t")
    dfs.append(cur_df)


all_reviews = pd.concat(dfs, ignore_index=True)

# Save the dataframe
all_reviews.to_csv("./scraped_reviews.csv", sep="\t", index=False)

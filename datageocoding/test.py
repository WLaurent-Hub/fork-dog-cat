import pandas as pd
import os
from joblib import Parallel, delayed

def read_and_write(year):
    df = pd.read_csv(os.path.join(f"./data/data{year}.csv"), header=0, sep=",")
    df.to_csv(f"new_data{year}.csv", index=False, encoding="utf-8")

list_year = ["2013", "2014", "2015", "2016", "2019"]
Parallel(n_jobs=5, prefer="threads")(delayed(read_and_write)(year) for _, year in enumerate(list_year))


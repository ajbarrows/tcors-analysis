#!/usr/bin/env python3

import os
import pandas as pd

def grab_header(fpath: str) -> pd.DataFrame:

    NROWS=8

    df = pd.read_csv(fpath, header=None, nrows=NROWS)
    df = df.iloc[:, 0:2].T

    # header
    head = df.iloc[0]
    df = df[1:]
    df.columns = head

    return df.reset_index(drop=True)

def grab_data(fpath: str) -> pd.DataFrame:

    SKIPROWS=17
    df = pd.read_csv(fpath, skiprows=SKIPROWS)

    return df.reset_index(drop=True)

def merge_header(header: pd.DataFrame, data: pd.DataFrame) -> pd.DataFrame:

    df = pd.concat([header, data], axis=1)
    df = (df
          .ffill()
          .drop_duplicates())

    return df

def assemble_stroop(fpath='../data/01_raw/', outpath= '../data/02_processed/stroop_concat.csv', rtn_output=False) -> pd.DataFrame:
    dfs = []
    collections = ['Stroop_Baseline2', 'Stroop_Week2', 'Stroop_Week6', 'Stroop_Week12']

    for collection in collections:

        dir = os.path.join(fpath, collection)
        files = os.listdir(dir)

        for f in files:
            
            infile = os.path.join(dir, f)

            tmp_head = grab_header(infile)
            tmp_data = grab_data(infile)
            tmp_merged = merge_header(tmp_head, tmp_data)

            dfs.append(tmp_merged)
    
    df = pd.concat(dfs)
    df.to_csv(outpath, index=False)

    if rtn_output:
        return df


if __name__ == "__main__":

    assemble_stroop()
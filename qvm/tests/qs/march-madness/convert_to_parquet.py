#!/usr/bin/env python3

import os

import pandas as pd


if __name__ == "__main__":
    for root, folders, files in os.walk("MDataFiles_Stage2"):
        for fname in files:
            if not fname.endswith(".csv"):
                continue
            fpath = os.path.join(root, fname)
            try:
                df = pd.read_csv(fpath)
                df.to_parquet(fpath.rsplit(".", 1)[0] + ".parquet")
                os.remove(fpath)
            except Exception as e:
                print(f"Failed on {fname}: {e}")
                continue

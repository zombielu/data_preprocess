import os

import numpy as np
import pandas as pd

# source_folder = '/Users/weilin/Downloads/raw_data/Archive'
source_folder = '/Users/weilinwu/Documents/data/raw_csv'
# file = '/Users/weilin/Downloads/raw_data/Archive/glbx-mdp3-20180401-20180430.trades.csv'
# file = '/Users/weilin/Downloads/raw_data/Archi/ve/glbx-mdp3-20190101-20190131.trades.csv'
# file = '/Users/weilin/Downloads/raw_data/Archive/glbx-mdp3-20220520.trades.csv'
# df = pd.read_csv(file)
# ts_col = df['ts_recv']
# dt_col = pd.to_datetime(ts_col, unit='ns', utc=True)
# ts_final = dt_col.astype(str).str.replace(" ", "T", regex=False).str.replace("+00:00", "Z", regex=False)
#
# print(f"############original nano:")
# print(ts_col)
# print(f"############ nano to datetime:")
# print(dt_col)
#
# print(f"############ nano to datetime:")
# print(ts_final)
#
# df['ts_recv'] = ts_final
# df.to_csv('test.csv', index=False)

for filename in os.listdir(source_folder):
    if filename.endswith('.csv'):
        # processed_file_path = os.path.join(source_folder, f"{filename}.bak")
        # if os.path.isfile(processed_file_path):
        #     print(f"{filename} has been processed")
        #     continue
        print(f"processing {filename}")
        file_path = os.path.join(source_folder, filename)
        df = pd.read_csv(file_path)
        ts_col = df['ts_event']

        if pd.api.types.is_object_dtype(ts_col):
            print("Skip: data type is correct.")
            continue
        dt_col = pd.to_datetime(ts_col, unit='ns', utc=True)
        ts_final = dt_col.astype(str).str.replace(" ", "T", regex=False).str.replace("+00:00", "Z", regex=False)

        invalid_mask = dt_col.isna()
        num_invalid = invalid_mask.sum()

        if num_invalid > 0:
            print(f"!!!!!!!!!!!!!!!!!Warning: there are {num_invalid} values in "
                  f"ts_recv fail to convert, replace it with NaN.")
            ts_final = ts_final.where(~invalid_mask, np.nan)

        df['ts_event'] = ts_final

        temp_file = file_path + '.tmp'
        # backup_file = file_path + '.bak'

        df.to_csv(temp_file, index=False)
        #
        # if not os.path.exists(backup_file):
        #     os.rename(file_path, backup_file)

        os.rename(temp_file, file_path)
        print(f":) :) :) safely wrote: {file_path}")
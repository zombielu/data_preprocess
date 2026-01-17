import os
import pandas as pd
import glob

folder_path = '/Users/weilin/Downloads/raw_data/Archive'
csv_files = glob.glob(os.path.join(folder_path, '*.csv'))

id_to_symbol = {}
for file in csv_files:
    try:
        for chunk in pd.read_csv(file, usecols=['instrument_id', 'symbol'], chunksize=100_000):
            chunk = chunk.dropna(subset=['symbol'])

            for iid, sym in zip(chunk['instrument_id'], chunk['symbol']):
                if iid not in id_to_symbol:
                    id_to_symbol[iid] = sym

    except Exception as e:
        print(f"{file}-fail to process-error:{e}")

map_df = pd.DataFrame(id_to_symbol.items(), columns=['instrument_id', 'symbol'])
map_df.to_csv('instrument_to_symbol.csv', index=False)
import os
import pandas as pd
import glob

mapping_df = pd.read_csv('instrument_to_symbol.csv')
id2symbol = dict(zip(mapping_df['instrument_id'],mapping_df['symbol']))

not_found_records = []

folder_path = '/Users/weilin/Downloads/raw_data/Archive'
csv_files = glob.glob(os.path.join(folder_path, '*.csv'))
filled_folder = '/Users/weilin/Downloads/filled_files'

for file in csv_files:
    df = pd.read_csv(file)
    if 'symbol' in df.columns:
        continue
    else:
        df['symbol'] = None
        df['symbol'] = df['instrument_id'].map(id2symbol)
        not_found = df[df['symbol'].isnull()]['instrument_id']
        for instrument_id in not_found.unique():
            not_found_records.append({'file': os.path.basename(file), 'instrument_id': instrument_id})
    filled_file_path = os.path.join(filled_folder, os.path.basename(file))
    df.to_csv(filled_file_path, index= False, encoding='utf-8-sig')

if not_found_records:
    nf_df = pd.DataFrame(not_found_records)
    nf_df.to_csv('not_found_instrument_ids.csv', index=False, encoding='utf-8-sig')
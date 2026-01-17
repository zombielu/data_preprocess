import os
import pandas as pd
import glob

folder_path = '/Users/weilin/Downloads/raw_data/Archive'
csv_files = glob.glob(os.path.join(folder_path, '*.csv'))

with open('/Users/weilin/Downloads/output.txt', 'w', encoding='utf-8') as f:
    for file in csv_files:
        try:
            df = pd.read_csv(file, usecols=['symbol', 'instrument_id'])
            symbol_group = df.groupby('symbol')['instrument_id'].nunique()
            symbol_valid = symbol_group.max() == 1

            instrument_group = df.groupby('instrument_id')['symbol'].nunique()
            instrument_valid = instrument_group.max() == 1


            if symbol_valid and instrument_valid:
                print('ok')
            else:

                print(f'!!!!!!!!!!!!!!!!!!!!!!{file} not valid!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')

        except Exception as e:
            f.write(file + '\n')
            print(f"{file}-fail to process-error:{e}")


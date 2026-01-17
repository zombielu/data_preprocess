import os
import pandas as pd
import shutil

source_folder = '/Users/weilin/Downloads/raw_data/Archive'
checked_folder = '/Users/weilin/Downloads/checked_data'

with_symbol = os.path.join(checked_folder, 'with_symbol')
without_symbol = os.path.join(checked_folder, 'without_symbol')

os.makedirs(with_symbol,exist_ok=True)
os.makedirs(without_symbol,exist_ok=True)

for filename in os.listdir(source_folder):
    # print(filename)
    if filename.endswith('.csv'):
        file_path = os.path.join(source_folder, filename)
        try:
            df = pd.read_csv(file_path, nrows=0)
            columns = [col.strip().lower() for col in df.columns]
            if 'symbol' in columns:
                shutil.copy(file_path, os.path.join(with_symbol, filename))
                print(f"copy {filename} to with_symbol")
            else:
                shutil.copy(file_path, os.path.join(without_symbol, filename))
                print(f"copy {filename} to without_symbol")

        except Exception as e:
            print(f"{filename}-fail to process-error:{e}")
import os

import numpy as np
import pandas as pd

def uniform_time_column(source_folder: str) -> None:
    """
    Normalize the `ts_event` column format for all CSV files in source_folder.

    This function iterates through all `.csv` files in `source_folder` and
    ensures the `ts_event` column is stored as an ISO 8601 UTC timestamp string
    in the format:

        YYYY-MM-DDTHH:MM:SS.sssssssssZ

    Processing logic:
    - If `ts_event` is already of object/string dtype, the file is skipped.
    - Otherwise, `ts_event` is interpreted as a nanosecond Unix timestamp
      and converted using `pd.to_datetime(..., unit='ns', utc=True)`.
    - Converted timestamps are formatted to ISO 8601 with `T` separator and
      `Z` suffix.
    - Any values that fail conversion are replaced with NaN and reported.

    File safety:
    - The original CSV file is overwritten atomically using a temporary file
      (`.tmp`) to reduce the risk of partial writes.

    Args:
        source_folder (str):
            Path to a folder containing CSV files. Each CSV file must contain
            a `ts_event` column.

    Returns:
        None

    """
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
                      f"ts_event fail to convert, replace it with NaN.")
                ts_final = ts_final.where(~invalid_mask, np.nan)

            df['ts_event'] = ts_final

            temp_file = file_path + '.tmp'
            # backup_file = file_path + '.bak'

            df.to_csv(temp_file, index=False)
            # create backup of the original file if needed.
            # if not os.path.exists(backup_file):
            #     os.rename(file_path, backup_file)

            os.rename(temp_file, file_path)
            print(f":) :) :) safely wrote: {file_path}")

if __name__ == '__main__':
    source_folder = '/Users/weilinwu/Documents/data/raw_csv'
    uniform_time_column(source_folder)
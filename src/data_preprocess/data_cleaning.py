import os
import numpy as np
import pandas as pd
import re

# ISO 8601 regex (simplified)
iso_pattern = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z")

def convert_ts(df: pd.DataFrame, col_name: str = "ts_event") -> pd.DataFrame:
    """
    Convert a timestamp column to ISO 8601 format (UTC, 'YYYY-MM-DDTHH:MM:SSZ').
    Supports integer/float nanosecond timestamps and object/string columns.
    Safely overwrites the original file.

    Parameters:
        file_path (str): Path to the CSV file to process
        col_name (str): Name of the timestamp column (default: 'ts_event')
    """

    if col_name not in df.columns:
        print(f"Skip: no column '{col_name}' in {file_path}")
        return

    ts_col = df[col_name]

    # Determine if conversion is needed
    if pd.api.types.is_integer_dtype(ts_col) or pd.api.types.is_float_dtype(ts_col):
        need_conversion = True
    else:
        try:
            dt_col = pd.to_datetime(ts_col, utc=True)
            if ts_col.dropna().apply(lambda x: bool(iso_pattern.fullmatch(str(x)))).all():
                need_conversion = False
            else:
                need_conversion = True
        except Exception:
            need_conversion = True

    if not need_conversion:
        print(f"Skip: column '{col_name}' already in correct ISO 8601 format for {file_path}")
        return

    # Conversion
    if pd.api.types.is_integer_dtype(ts_col) or pd.api.types.is_float_dtype(ts_col):
        dt_col = pd.to_datetime(ts_col, unit='ns', utc=True)
    else:
        dt_col = pd.to_datetime(ts_col, utc=True, errors='coerce')

    ts_final = dt_col.astype(str).str.replace(" ", "T", regex=False).str.replace("+00:00", "Z", regex=False)

    # Handle invalid timestamps
    invalid_mask = dt_col.isna()
    num_invalid = invalid_mask.sum()
    if num_invalid > 0:
        print(f"!!!!!!!! Warning: {num_invalid} values in '{col_name}' failed to convert. Replaced with NaN.")
        ts_final = ts_final.where(~invalid_mask, np.nan)

    # Update column and write safely
    df[col_name] = ts_final
    return df

def uniform_col_to_bigint(
    df: pd.DataFrame,
    col_name: str = "price"
) -> pd.DataFrame:
    """
    Uniform a price column to bigint (pandas nullable Int64).

    Rules:
    - Accept int / float / numeric string
    - Non-convertible values -> <NA>
    - Does NOT modify input df in-place
    """

    if col_name not in df.columns:
        return df

    df = df.copy()

    # Step 1: convert everything to numeric (invalid -> NaN)
    col_num = pd.to_numeric(df[col_name], errors="coerce")

    # Step 2: convert to pandas nullable bigint
    df[col_name] = col_num.astype("Int64")

    return df

# Example usage for a folder of CSVs

def clean_all_columns(source_folder):
    for filename in os.listdir(source_folder):
        if filename.endswith('.csv'):
            file_path = os.path.join(source_folder, filename)
            df = pd.read_csv(file_path)

            df = convert_ts(df, col_name='ts_event')
            df = uniform_col_to_bigint(df, col_name='price')

            temp_file = file_path + '.tmp'
            df.to_csv(temp_file, index=False)
            os.rename(temp_file, file_path)
            print(f"Successfully wrote: {file_path}\n")


if __name__ == "__main__":
    source_folder = '/Users/weilinwu/Documents/data/raw_csv'
    clean_all_columns(source_folder)
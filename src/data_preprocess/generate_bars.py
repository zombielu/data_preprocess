from datetime import datetime, time, date, timedelta
import os
import pandas as pd
import pytz
from pathlib import Path

NY = pytz.timezone("America/New_York")
UTC = pytz.UTC

def generate_1_day_bars(input_folder_path:str, output_folder_path:str) -> None:
    """
    Generate daily OHLCV (Open, High, Low, Close, Volume) bars from raw parquet trade files.

    This function reads trade data from parquet files within the given input folder,
    converts timestamps to the New York timezone, aggregates trades into daily OHLCV bars,
    and saves the result as a parquet file in the output folder.

    Args:
        input_folder_path: Path to the folder containing input parquet files with trade data.
        output_folder_path: Path to the folder where the aggregated parquet file will be saved.

    Returns:
        None
        The function saves the aggregated OHLCV data to a parquet file in the
        specified output folder.
    """
    input_folder_path = Path(input_folder_path)
    ohlcv_rows = []
    first_date, last_date = None, None
    for file in input_folder_path.glob("*.parquet"):
        print(f"Processing {file}")
        df = pd.read_parquet(file, engine="pyarrow", columns=("ts_event", "price", "size"))
        df = df.sort_values("ts_event")
        df["price"] = df["price"] // 1e7
        df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True, format='ISO8601').dt.tz_convert(NY)
        ny_time = df["ts_event"].iloc[-1].date()
        if first_date is None or ny_time < first_date:
            first_date = ny_time
        if last_date is None or ny_time > last_date:
            last_date = ny_time
        ohlcv_row = {
            "time": ny_time,
            "open": df["price"].iloc[0],
            "high": df["price"].max(),
            "low": df["price"].min(),
            "close": df["price"].iloc[-1],
            "volume": df["size"].sum(),
        }
        ohlcv_rows.append(ohlcv_row)
    if ohlcv_rows:
        ohlcv_rows.sort(key=lambda x: x["time"])
    one_day_df = pd.DataFrame(ohlcv_rows)
    one_day_file_name = f"{first_date.strftime('%Y%m%d')}-{last_date.strftime('%Y%m%d')}_1_day.parquet"
    one_day_file_path = os.path.join(output_folder_path, one_day_file_name)
    one_day_df.to_parquet(one_day_file_path, engine="pyarrow", index=False)

def get_time_range(time_type: str, target_date: datetime.date) -> tuple[datetime, datetime]:
    """
    Get the start and end timestamps of a trading session in New York time.

    Depending on the specified session type, this function returns a timezone-aware
    datetime range for either the electronic session or the regular session on a given date.

    Args:
        time_type:
        The type of trading session. Must be one of:
        - "electronic" : Session runs from 6:00 PM (previous day) to 4:15 PM (current day).
        - "regular"    : Session runs from 9:30 AM to 4:15 PM (current day).
        target_date: The trading date for which the session range is computed.

    Returns:
        A tuple containing the start and end datetimes of the session, both localized
        to the New York timezone.
    """
    if time_type == "electronic":
        start_date = target_date - timedelta(days=1)
        start_time = datetime.combine(start_date, time(18,0))
        ny_start = NY.localize(start_time)

        end_time = datetime.combine(target_date, time(16, 15))
        ny_end = NY.localize(end_time)

    if time_type == "regular":
        start_time = datetime.combine(target_date, time(9, 30))
        ny_start = NY.localize(start_time)

        end_time = datetime.combine(target_date, time(16, 15))
        ny_end = NY.localize(end_time)
    return ny_start, ny_end



def generate_min_bars(input_folder_path:str, output_folder_path:str, time_type:str, time_slot:int) -> None:
    """
    Generate aggregated OHLCV (Open, High, Low, Close, Volume) bars at a specified
    time interval from raw parquet trade files.

    This function reads trade data from parquet files within the given input folder,
    converts timestamps to the New York timezone, filters data within a trading
    session (as defined by `time_type`), aggregates trades into OHLCV bars at the
    specified minute interval, and saves the final result as a parquet file in
    the output folder.

    Args:
        input_folder_path: Path to the folder containing input parquet files with trade data.
        output_folder_path: Path to the folder where the aggregated parquet file will be saved.
        time_type: Type of trading session to filter by (e.g., "regular", "electronic").
        Passed to the helper function `get_time_range` to determine valid session boundaries.
        time_slot: Time interval (in minutes) for resampling the data (e.g., 1 for 1-minute bars,
        5 for 5-minute bars).

    Returns:
        None
        The function saves the aggregated OHLCV data to a parquet file in the
        specified output folder.
    """
    input_folder_path = Path(input_folder_path)
    ohlcv_rows = []
    first_date, last_date = None, None
    for file in input_folder_path.glob("*.parquet"):
        print(f"Processing {file}")
        df = pd.read_parquet(file, engine="pyarrow", columns=("ts_event", "price", "size"))
        df = df.sort_values("ts_event")
        df["price"] = df["price"] // 1e7
        df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True, format='ISO8601').dt.tz_convert(NY)
        ny_date = df["ts_event"].iloc[-1].date()
        if first_date is None or ny_date < first_date:
            first_date = ny_date
        if last_date is None or ny_date > last_date:
            last_date = ny_date

        # aggregate data to minute-level
        minute_level = str(time_slot) + "min"
        df["minute"] = df["ts_event"].dt.floor(minute_level)

        start, end = get_time_range(time_type, ny_date)
        df = df[(df["ts_event"] >= start) & (df["ts_event"] < end)]

        ohlcv = df.groupby("minute").agg(
            time=("minute", "first"),
            open=("price", "first"),
            high=("price", "max"),
            low=("price", "min"),
            close=("price", "last"),
            volume=("size", "sum"),
        ).reset_index(drop=True)

        ohlcv_rows.append(ohlcv)
    if ohlcv_rows:
        final_df = pd.concat(ohlcv_rows).sort_values("time")
    final_file_name = f"{first_date.strftime('%Y%m%d')}-{last_date.strftime('%Y%m%d')}_{time_slot}_min.parquet"
    final_file_path = os.path.join(output_folder_path, final_file_name)
    final_df.to_parquet(final_file_path, engine="pyarrow", index=False)
    # final_df.to_csv(final_file_path, index=False)


def generate_hour_bars(input_folder_path:str, output_folder_path:str, time_type:str, time_slot: int) -> None:
    """
    Generate aggregated OHLCV (Open, High, Low, Close, Volume) bars at a specified
    hourly interval from raw parquet trade files.

    This function reads trade data from parquet files within the given input folder,
    converts timestamps to the New York timezone, filters data within a trading
    session (as defined by `time_type`), aggregates trades into OHLCV bars at the
    specified hourly interval, and saves the final result as a parquet file in the
    output folder.

    Args:
        input_folder_path: Path to the folder containing input parquet files with trade data.
        output_folder_path: Path to the folder where the aggregated parquet file will be saved.
        time_type: Type of trading session to filter by (e.g., "regular", "electronic").
        Passed to the helper function `get_time_range` to determine valid session boundaries.
        time_slot: Time interval (in hours) for resampling the data
        (e.g., 1 for 1-hour bars, 4 for 4-hour bars).

    Returns:
        None
        The function saves the aggregated OHLCV data to a parquet file in the
        specified output folder.
    """
    input_folder_path = Path(input_folder_path)
    ohlcv_rows = []
    first_date, last_date = None, None
    for file in input_folder_path.glob("*.parquet"):
        print(f"Processing {file}")
        df = pd.read_parquet(file, engine="pyarrow", columns=("ts_event", "price", "size"))
        df = df.sort_values("ts_event")
        df["price"] = df["price"] // 1e7
        df["ts_event"] = pd.to_datetime(df["ts_event"], utc=True, format='ISO8601').dt.tz_convert(NY)
        ny_date = df["ts_event"].iloc[-1].date()
        if first_date is None or ny_date < first_date:
            first_date = ny_date
        if last_date is None or ny_date > last_date:
            last_date = ny_date

        start, end = get_time_range(time_type, ny_date)
        df = df[(df["ts_event"] >= start) & (df["ts_event"] < end)]

        # aggregate data to hour-level
        # hour_level = str(time_slot) + "h"
        # df["hour"] = df["ts_event"].dt.floor(hour_level)
        starting_time = df["ts_event"].iloc[0].floor('h')
        df["bin"] = ((df["ts_event"] - starting_time) / pd.Timedelta(hours=time_slot)).astype(int)


        ohlcv = df.groupby("bin").agg(
            open=("price", "first"),
            high=("price", "max"),
            low=("price", "min"),
            close=("price", "last"),
            volume=("size", "sum"),
        ).reset_index()

        ohlcv.insert(0, "time", starting_time + pd.to_timedelta(ohlcv["bin"] * time_slot, unit="h"))
        ohlcv = ohlcv.drop(columns=["bin"])

        ohlcv_rows.append(ohlcv)
    if ohlcv_rows:
        final_df = pd.concat(ohlcv_rows).sort_values("time")
    final_file_name = f"{first_date.strftime('%Y%m%d')}-{last_date.strftime('%Y%m%d')}_{time_slot}_hour.parquet"
    final_file_path = os.path.join(output_folder_path, final_file_name)
    final_df.to_parquet(final_file_path, engine="pyarrow", index=False)
    # final_df.to_csv(final_file_path, index=False)



if __name__ == "__main__":
    # input_folder_path = '/Users/weilinwu/Downloads/trade_data'
    input_folder_path = '/Users/weilinwu/Downloads/raw_silver'
    output_folder_path = '/Users/weilinwu/Downloads/bar/electronic/1_day'
    generate_1_day_bars(input_folder_path, output_folder_path)
    # generate_hour_bars(input_folder_path, output_folder_path, "electronic", 4)
    # generate_min_bars(input_folder_path, output_folder_path, "electronic", 30)
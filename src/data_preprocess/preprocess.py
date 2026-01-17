import os
import re

import pandas as pd
import glob
import pytz
from datetime import datetime, timedelta, time

from dateutil import parser
from tqdm import tqdm

import pyarrow as pa
import pyarrow.parquet as pq

# pd.set_option('display.max_columns', None)
NY = pytz.timezone("America/New_York")
UTC = pytz.UTC
TRADE_SCHEMA_V1 = pa.schema([
    ("ts_recv", pa.string()),
    ("ts_event", pa.string()),
    ("rtype", pa.int64()),
    ("publisher_id", pa.int64()),
    ("instrument_id", pa.int64()),
    ("action", pa.string()),
    ("side", pa.string()),
    ("depth", pa.int64()),
    ("price", pa.int64()),
    ("size", pa.int64()),
    ("flags", pa.int64()),
    ("ts_in_delta", pa.int64()),
    ("sequence", pa.int64()),
    ("symbol", pa.string()),
])

TRADE_SCHEMA_V2 = pa.schema([
    ("ts_recv", pa.string()),
    ("ts_event", pa.string()),
    ("rtype", pa.int64()),
    ("publisher_id", pa.int64()),
    ("instrument_id", pa.int64()),
    ("action", pa.string()),
    ("side", pa.string()),
    ("depth", pa.int64()),
    ("price", pa.int64()),
    ("size", pa.int64()),
    ("flags", pa.int64()),
    ("ts_in_delta", pa.int64()),
    ("sequence", pa.int64()),
])

def parse_to_ny_datetime(val: pd.Timestamp | datetime | str) -> datetime:
    """
    Uniform the values in time column of the reference file by converting them
    into datetime type with New York timezone.

    Args:
        val: the time value in the time column of the reference file

    Returns: the time value in datetime type with New York timezone

    """
    if isinstance(val, pd.Timestamp):
        if val.tzinfo is None:
            return NY.localize(val.to_pydatetime())
        else:
            return val.tz_convert(NY)

    if isinstance(val, datetime):
        if val.tzinfo is None:
            return NY.localize(val)
        else:
            return val.astimezone(NY)

    if isinstance(val, str):
        dt_naive = parser.parse(val, dayfirst=True)
        if dt_naive.tzinfo is None:
            dt = NY.localize(dt_naive)
        else:
            dt = dt_naive.astimezone(NY)
        return dt


def get_utc_start_end(date: datetime.date) -> tuple[datetime, datetime]:
    """
    This function is used to find the time span of a specific date in stock
    market. Given a specific date in New York timezone, calculate the
    corresponding start and end time in UTC timezone.
    For example:
    For a date ny_time = datetime(2018,4,2,tzinfo=NY):
    First, translate it into the opening time and closing time of the stock
    market as 2018-04-01 18:00:00-04:00 and 2018-04-02 17:00:00-04:00.
    Then, convert these two time into UTC timezone as 2018-04-01 22:00:00+00:00
    and 2018-04-02 21:00:00+00:00

    Args:
        date: a date in New York timezone

    Returns: start and end time in UTC timezone

    """
    ny_start = NY.localize(
        datetime.combine(date - timedelta(days=1), time(18, 0)))
    utc_start = ny_start.astimezone(UTC)

    ny_end = NY.localize(datetime.combine(date, time(17, 0)))
    utc_end = ny_end.astimezone(UTC)
    return utc_start, utc_end


def parse_file_dates(filename: str) -> tuple[datetime.date, datetime.date]:
    """
    Parse the date from filename.
    For example:
    For filename contains two dates like 'glbx-mdp3-20180408-20180430.trades.csv',
    it will return datetime.date(2018, 4, 8), datetime.date(2018, 4, 30)
    For filename contains only one date like 'glbx-mdp3-20180406.trades.csv',
    it will return datetime.date(2018, 4, 6), datetime.date(2018, 4, 6)

    Args:
        filename: file name of the files in input folder.

    Returns: the start and end date during which the file covers the trade info.

    """
    match_range = re.search(r'(\d{8})-(\d{8})', filename)
    if match_range:
        start_str, end_str = match_range.groups()
        start_date = datetime.strptime(start_str, '%Y%m%d').date()
        end_date = datetime.strptime(end_str, '%Y%m%d').date()
        return start_date, end_date

    match_single = re.search(r'(\d{8})', filename)
    if match_single:
        date_str = match_single.group(1)
        date = datetime.strptime(date_str, '%Y%m%d').date()
        return date, date
    return None, None


def find_related_files(
        file_list: list[str],
        utc_start: datetime,
        utc_end: datetime,
) -> list[str]:
    """
    Check the filenames in file_list, find the files that may contain the trade
    info during the time period between utc_start and utc_end.

    Args:
        file_list: filename list of all the files in input folder
        utc_start: the start date we are trying to find the trade info
        utc_end: the end date we are trying to find the trade info

    Returns: a list of filenames that contain the trade info during the period
    between utc_start and utc_end

    """
    matched_files = set()
    for f in file_list:
        start_date, end_date = parse_file_dates(f)
        if start_date is None:
            continue
        if start_date <= utc_start.date() <= end_date:
            matched_files.add(f)
        if start_date <= utc_end.date() <= end_date:
            matched_files.add(f)
    return list(matched_files)

def is_processed(
        output_folder_path: str,
        reference_date: datetime.date,
) -> bool:
    """
    Check if given reference_date has already been processed and generated
    the corresponding output parquet file.

    Args:
        output_folder_path: path of the output folder which collects all the
        output parquet files.
        reference_date: a date(row) in reference file that needs to be processed.

    Returns: True if there exists an output file for the reference_date.
    Otherwise, return False.

    """
    date_str = reference_date.strftime('%Y%m%d')
    file_path = os.path.join(output_folder_path, f"{date_str}.parquet")
    return os.path.isfile(file_path)



def get_related_trade_records(
        reference_file_path: str,
        input_folder_path: str,
        output_folder_path: str,
) -> None:
    """
    For each date(row) in reference file, find the related files from the files
    in input folder. Then, extract the related trade records from those
    related files. Finally, combine the related trade records into one
    file and save the file in the output folder. Do the above steps for
    each date(row) in reference file, save all the trade record files in
    the output folder.

    Args:
        reference_file_path: path of the reference file
        input_folder_path: path of the input folder or raw data folder which
        contains csv files downloaded from website.
        output_folder_path: path of the output folder where you want to
        receive all the generated trade_records files (this folder is also called
        raw_silver)

    Returns: None

    """
    csv_files = glob.glob(os.path.join(input_folder_path, '*.csv'))
    filename_list = [os.path.basename(f) for f in csv_files]

    os.makedirs(output_folder_path, exist_ok=True)

    reference_df = pd.read_csv(reference_file_path, parse_dates=['time'])
    reference_df['time'] = reference_df['time'].apply(parse_to_ny_datetime)

    # i = 0
    # reference_contracts = {}
    # problem_days = {}
    for _, row in tqdm(reference_df.iterrows(), total=len(reference_df)):
        reference_date = row['time'].date()
        # Note: by Now, our input folder only contains csv files in the time span
        # of 2018-04-02 to 2025-06-30, change it when there are more data files.
        if reference_date < datetime(2017, 4, 3, 0, 0, 0).date() \
                or reference_date > datetime(2025, 9, 28, 0, 0, 0).date():
            continue

        if is_processed(output_folder_path, reference_date):
            continue

        utc_start, utc_end = get_utc_start_end(reference_date)
        print(
            f'Processing time span of {utc_start} to {utc_end} in UTC timezone'
        )
        o, h, l, c = row['open'], row['high'], row['low'], row['close']
        print(
            f'Reference open, high, low, close values: {o, h, l, c}'
        )
        related_files = find_related_files(filename_list, utc_start, utc_end)
        print(
            f'Found related raw data files from input folder: {related_files}'
        )

        # only filter the trade records that are in the time period between
        # utc_start and utc_end.
        candidate_data = []
        for fname in related_files:
            fpath = os.path.join(input_folder_path, fname)

            for chunk in pd.read_csv(fpath, chunksize=100_000):
                if not pd.api.types.is_object_dtype(chunk['ts_event']):
                    ts_event = pd.to_datetime(chunk['ts_event'], unit='ns', utc=True)
                    chunk['ts_event'] = ts_event.astype(str).str.replace(" ", "T",
                               regex=False).str.replace("+00:00", "Z", regex=False)
                    ts_recv = pd.to_datetime(chunk['ts_recv'], unit='ns',
                                            utc=True)
                    chunk['ts_recv'] = ts_recv.astype(str).str.replace(" ",
                                      "T", regex=False).str.replace("+00:00", "Z", regex=False)
                chunk['ts_utc'] = pd.to_datetime(chunk['ts_event'], utc=True, format='ISO8601')
                chunk = chunk[(chunk['ts_utc'] >= utc_start) & (
                        chunk['ts_utc'] < utc_end)]
                candidate_data.append(chunk)

        full_data = pd.concat(candidate_data, ignore_index=True)
        group = full_data.groupby('instrument_id')
        volume_sum = group['size'].sum()
        max_instrument_id = volume_sum.idxmax()
        # group = full_data.groupby('symbol')

        max_ohlc = 0
        candidate_volume = [None, None]
        for inst_id, group_df in group:
            group_df = group_df.sort_values('ts_event')
            open_ = group_df.iloc[0]['price']
            close_ = group_df.iloc[-1]['price']
            high_ = group_df['price'].max()
            low_ = group_df['price'].min()
            volume_ = group_df['size'].sum()
            # print(inst_id, open_, high_, low_, close_, volume_)

            ohlc_flag = [
                open_ == o * 10 ** 9,
                high_ == h * 10 ** 9,
                low_ == l * 10 ** 9,
                close_ == c * 10 ** 9
            ]

            if sum(ohlc_flag) >= 3:
                if sum(ohlc_flag) > max_ohlc:
                    candidate_volume[0], candidate_volume[1] = inst_id, volume_
                else:
                    print(
                        f"WARNING: {reference_date} exists multiple contracts "
                        f"that match three values in ohlc.")
                    # dealing with the case where there are multiple contracts
                    # that have three values in ohlc match with the values in
                    # reference file. Pick the contract with largest volume.
                    if volume_ > candidate_volume[1]:
                        candidate_volume[0], candidate_volume[
                            1] = inst_id, volume_
            elif sum(ohlc_flag) == 2:
                if sum(ohlc_flag) < max_ohlc:
                    continue
                else:
                    if max_ohlc < 2:
                        candidate_volume[0], candidate_volume[1] = inst_id, volume_
                        saved_info_2 = [open_, high_, low_, close_, volume_]
                    else:
                    # dealing with the case where there are multiple contracts
                    # satisfy the condition that only two values of ohlc
                    # match with the values in reference file.
                        if volume_ > candidate_volume[1]:
                            candidate_volume[0], candidate_volume[1] = inst_id, volume_
                            saved_info_2 = [open_, high_, low_, close_, volume_]
            elif sum(ohlc_flag) == 1:
                if sum(ohlc_flag) < max_ohlc:
                    continue
                else:
                    if max_ohlc < 1:
                        candidate_volume[0], candidate_volume[1] = inst_id, volume_
                        saved_info_1 = [open_, high_, low_, close_, volume_]
                    else:
                        # dealing with the case where there are multiple contracts
                        # satisfy the condition that only one value of ohlc
                        # match with the values in reference file.
                        if volume_ > candidate_volume[1]:
                            candidate_volume[0], candidate_volume[1] = inst_id, volume_
                            saved_info_1 = [open_, high_, low_, close_, volume_]

            max_ohlc = max(max_ohlc, sum(ohlc_flag))


        if max_ohlc >= 1:
            if max_ohlc == 1:
                print(
                    f"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
                    f"WARNING: {reference_date} matches only one value "
                    f"in ohlcv, {saved_info_1}"
                )
                # problem_days[reference_date] = saved_info_1
            if max_ohlc == 2:
                print(
                    f"!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
                    f"WARNING: {reference_date} matches only two values "
                    f"in ohlcv, {saved_info_2}"
                )
                # problem_days[reference_date] = saved_info_2
            inst_id = candidate_volume[0]
            print(f"Found related contract {inst_id} for {reference_date}")
            # reference_contracts[reference_date] = inst_id
            # output_records = full_data[full_data['symbol'] == inst_id]
            output_records = full_data[full_data['instrument_id'] == inst_id]


        else:
            # raise Exception(f"{reference_date}-fail to find related contracts")
            print(f"OHLC all 4 numbers don't match. Use contract {max_instrument_id} "
                  f"with largest volume for {reference_date}")
            output_records = full_data[full_data['instrument_id'] == max_instrument_id]

        output_records = output_records.drop('ts_utc', axis=1)
        output_file = os.path.join(
                output_folder_path,
                f"{reference_date.strftime('%Y%m%d')}.parquet"
            )
            # output_file = os.path.join(output_folder_path, "testdata.csv")
        print(f"Created trade file {reference_date.strftime('%Y%m%d')}.parquet")
        # output_records.to_parquet(output_file, engine='pyarrow', index=False)
            # output_records.to_csv(output_file, index=False)
        if "symbol" in output_records.columns:
            schema = TRADE_SCHEMA_V1
        else:
            schema = TRADE_SCHEMA_V2

        table = pa.Table.from_pandas(
            output_records,
            schema=schema,
            preserve_index=False
        )

        pq.write_table(table, output_file)

        # i += 1
        # if i >= 1:
        #     break
    # reference_contracts_df = pd.DataFrame(reference_contracts.items(),
    #                                       columns=['date', 'instrument_id'])
    # reference_contracts_df.to_csv('reference_related_contracts.csv',
    #                               index=False)
    # problem_days_df = pd.DataFrame(problem_days.items(),
    #                                       columns=['date', 'ohlcv'])
    # problem_days_df.to_csv('problem_days.csv',
    #                               index=False)


if __name__ == "__main__":
    reference_file_path = '/Users/weilinwu/Downloads/ES_day_bar_20251001.csv'
    input_folder_path = '/Users/weilinwu/Documents/data/raw_csv'
    output_folder_path = '/Users/weilinwu/Downloads/output_folder'


    get_related_trade_records(
        reference_file_path,
        input_folder_path,
        output_folder_path)

    # ny_time = datetime(2020,4,15,tzinfo=NY)
    # print(get_utc_start_end(ny_time))

    # filename = 'glbx-mdp3-20180408-20180430.trades.csv'
    # print(parse_file_dates(filename))

    # test find_related_files
    # file_list = [
    #     'glbx-mdp3-20180406.trades.csv',
    #     'glbx-mdp3-20180408-20180430.trades.csv',
    #     'glbx-mdp3-20180507.trades.csv',
    # ]
    # ny_time = datetime(2018,4,8,tzinfo=NY)
    # utc_start, utc_end = get_utc_start_end(ny_time)
    # print(utc_start, utc_end)
    # matched_files = find_related_files(file_list,utc_start,utc_end)
    # print(matched_files)

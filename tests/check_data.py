import pandas as pd

def convert_ns_to_ny_time(df, ts_col):
    return (pd.to_datetime(df[ts_col], unit='ns', utc=True).dt.tz_convert('America/New_York').dt.strftime('%Y-%m-%d %H:%M:%S'))

df1 = pd.read_csv('/Users/weilin/Downloads/raw_data/Archive/glbx-mdp3-20240313.trades.csv')
df1['ts_ny'] = convert_ns_to_ny_time(df1, 'ts_recv')
df1.to_csv('/Users/weilin/Downloads/glbx-mdp3-20240313_with_ny_time.csv', index=False)

df2 = pd.read_csv('/Users/weilin/Downloads/raw_data/Archive/glbx-mdp3-20240314.trades.csv')
df2['ts_ny'] = convert_ns_to_ny_time(df2, 'ts_recv')
df2.to_csv('/Users/weilin/Downloads/glbx-mdp3-20240314_with_ny_time.csv', index=False)


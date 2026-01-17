import re

import pandas as pd
from tqdm import tqdm


file_path = "/Users/weilinwu/Documents/pycharm_prj/data_process/reference_related_contracts.csv"
df = pd.read_csv(file_path)

different_dates = {}
for _, row in tqdm(df.iterrows(), total=len(df)):
    row["instrument_id"] = row["instrument_id"].strip("[]").split(", ")
    # print(row["instrument_id"][0],row["instrument_id"][1])
    num = row["instrument_id"][0]
    match = re.search(r"\((\d+)\)", num)
    volume_max_id = int(match.group(1))
    ohlc_match_id = int(row["instrument_id"][1])
    if volume_max_id != ohlc_match_id:
        different_dates[row["date"]] = (volume_max_id, ohlc_match_id)

print(different_dates)# print(type(row["instrument_id"][1]))
df = pd.DataFrame([
    {
        "date": date,
        "volume_max_id": ids[0],
        "ohlc_match_id": ids[1]
    }
    for date, ids in different_dates.items()
])

# Save to CSV
df.to_csv("different_dates.csv", index=False)



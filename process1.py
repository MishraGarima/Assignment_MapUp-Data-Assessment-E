import argparse
import pandas as pd
import os

parser = argparse.ArgumentParser()
parser.add_argument("--to_process", default="evaluation_data/input/raw_data.parquet", help="Path to the Parquet file to be processed")
parser.add_argument("--output_dir", default="evaluation_data/output/process1", help="The folder to store the resulting CSV files")
args = parser.parse_args()

df = pd.read_parquet(args.to_process)

df['timestamp_new'] = pd.to_datetime(df['timestamp'])

#print(df.head())
#print(df.info())

for group,frame in df.groupby('unit'):
    trip_number = 0
    lst = []
    frame.drop('unit', axis=1, inplace=True)

    for i in range(len(frame)-1):
        lst.append([df.iloc[i]['latitude'],df.iloc[i]['longitude'],df.iloc[i]['timestamp']])
        consecutive_time_diff = (df.iloc[i+1]['timestamp_new'] - df.iloc[i]['timestamp_new']).seconds/(60*60)
        if (consecutive_time_diff > 7):
            temp_df = pd.DataFrame(lst, columns=['latitude','longitude','timestamp'])
            os.makedirs(args.output_dir, exist_ok=True)
            csv_name = f"{group}_{trip_number}.csv"
            temp_df.to_csv(os.path.join(args.output_dir, csv_name), index=False)
            trip_number = trip_number+1
            lst = []
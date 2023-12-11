import os
import json
import pandas as pd
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--to_process", default="evaluation_data/output/process2", help="Path to the Parquet file to be processed")
parser.add_argument("--output_dir", default="evaluation_data/output/process3", help="The folder to store the resulting CSV files")
args = parser.parse_args()

json_files = [f for f in os.listdir(args.to_process) if f.endswith('.json')]
os.makedirs(args.output_dir, exist_ok=True)
result_csv = os.path.join(args.output_dir, 'transformed_data.csv')

all_data = []
for json_file in json_files:
    json_path = os.path.join(args.to_process, json_file)
    with open(json_path, 'r') as file:
        json_data = json.load(file)
    #print(json_data['summary'].keys())
    if json_data['status']=="OK":
        temp = []
        temp.append(str(json_file).split("_")[0])
        temp.append(json_data['route']['costs']['tag'])
        temp.append(json_data['route']['costs']['cash'])
        temp.append(json_data['route']['costs']['licensePlate'])
        all_data.append(temp)

#print(all_data)
df = pd.DataFrame(all_data, columns = ['unit', 'tag_cost', 'cash_cost','license_plate_cost'])
df.to_csv(result_csv)

# Save the consolidated toll data to a CSV file
#save_to_csv(all_toll_data, output_dir)
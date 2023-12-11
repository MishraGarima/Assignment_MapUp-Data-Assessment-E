import argparse
import os
import requests
import concurrent.futures
from dotenv import load_dotenv

load_dotenv()

parser = argparse.ArgumentParser()
parser.add_argument("--to_process", default="evaluation_data/output/process1", help="Path to the Parquet file to be processed")
parser.add_argument("--output_dir", default="evaluation_data/output/process2", help="The folder to store the resulting CSV files")
args = parser.parse_args()

TOLLGURU_API_KEY = os.getenv('TOLLGURU_API_KEY')
TOLLGURU_API_URL = os.getenv('TOLLGURU_API_URL')
#print(TOLLGURU_API_KEY)

headers = {'x-api-key': TOLLGURU_API_KEY, 'Content-Type': 'text/csv'}
csv_files = [f for f in os.listdir(args.to_process) if f.endswith('.csv')]

def send_req(f_path):
    with open(f_path, 'rb') as file:
        response = requests.post(TOLLGURU_API_URL, data=file, headers=headers)

        filename = os.path.basename(f_path)
        os.makedirs(args.output_dir, exist_ok=True)
        json_output_path = os.path.join(args.output_dir, f"{os.path.splitext(filename)[0]}.json")
        with open(json_output_path, 'w') as json_file:
            json_file.write(response.text)


with concurrent.futures.ThreadPoolExecutor() as executor:
    executor.map(lambda file: send_req(os.path.join(args.to_process, file)), csv_files)
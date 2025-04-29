import glob
import json
from tqdm import tqdm
import srsly
import os

input_dir = "/Users/wjm55/data/artic-api-data/json/artworks/"
output_file = "/Users/wjm55/data/chicago-provenance/artworks-with-provenance.jsonl"

os.makedirs(os.path.dirname(output_file), exist_ok=True)

output_data = []
for file in tqdm(glob.glob(input_dir + "*.json")):
    with open(file, "r") as f:
        data = json.load(f)
        if data.get("provenance_text"):
            output_data.append(data)

srsly.write_jsonl(output_file, output_data)

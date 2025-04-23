import sys
import os
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
import yaml

from iiif_htr.caller import model_call, build_description
from iiif_htr.client import connect_to_client
import glob
from tqdm import tqdm
from datetime import datetime
from pydantic import BaseModel, Field
from typing import Optional, List, Literal, Union
import pandas as pd

import json

# Load terms from YAML
with open("structure.yaml", "r") as f:
    structure_data = yaml.safe_load(f)
PROVENANCE_METHODS = [item["term"] for item in structure_data.get("terms", [])] + ["Unknown"]

LOCATION = "us-central1"  # this is for Anthropic
PROJECT_ID = "cultural-heritage-gemini"
OUTPUT_DIR = "output"
MODELS = [
    # "google/gemini-1.5-pro-002",
    # "google/gemini-1.5-flash-002"
    "google/gemini-2.5-pro-preview-03-25"
]


df = pd.read_csv("/Users/wjm55/data/provenance/YUAG_provenance.csv")

# Sort by provenance length and get top 50 longest entries
df['provenance_length'] = df['Provenance'].str.len()
df = df.nlargest(50, 'provenance_length')
df = df.drop('provenance_length', axis=1)

PROVENANCES = df["Provenance"].tolist()
IDS = df["ObjectID"].tolist()



os.makedirs(f"{OUTPUT_DIR}", exist_ok=True)


class Actor(BaseModel):
    name: Union[Literal["Unknown"], str]

    # could be list of locations
    location: Union[Literal["Unknown"], str]

    birth_date: Union[Literal["Unknown"], str]
    death_date: Union[Literal["Unknown"], str]

    # change to allow the model to select multiple types
    type: Literal["Person", "Institution", "Collector", "Dealer", "Group", "Unknown"]

# Provenance activity
class Movement(BaseModel):
    source: Union[Literal["Unknown"], Actor]
    target: Union[Literal["Unknown"], Actor]
    start_time: Union[Literal["Unknown"], str]
    end_time: Union[Literal["Unknown"], str]

    # might be known but may be unknown - complex
    location: Union[Literal["Unknown"], str]

    # Handle Gaps
    method: Union[Literal[*PROVENANCE_METHODS], str]

class Provenance(BaseModel):
    movements: List[Movement]

# Build the schema description
schema_description = Provenance.model_json_schema()
print(schema_description)

# # Define the prompt
# prompt = "Convert this natural language description of provenance into a graph. Gaps in dates should use a unique Unknown node."

# # Format the prompt
# structured_prompt = f"""
#     {prompt}
    
#     You must respond with valid JSON that matches this schema:
#     {schema_description}
#     """



# connect to client
client = connect_to_client(LOCATION, PROJECT_ID)


def process_provenance(provenance, object_id, output_dir, models, ignore_existing=False):
    # Define the prompt
    prompt = "Convert this natural language description of provenance into a the schema provided. Fill any gaps in the timeline with Unknown Actors to indicate some kind of movement."

    # Format the prompt
    structured_prompt = f"""
        {prompt}:
        {provenance}
        
        You must respond with valid JSON that matches this schema:
        {schema_description}
        """
    try:  # Add error handling
        # Check if output file already exists
        output_file = f"{output_dir}/{object_id}.json"
        if ignore_existing and os.path.exists(output_file):
            return True

        transcriptions = []
        for model in models:
            transcription = model_call(
                structured_prompt,
                client,
                Provenance,
                model=model,
                method="entities",
                temperature=0.5,
            )
            transcription["model"] = model
            transcriptions.append(transcription)

        all_transcriptions = {
            "output": transcriptions,
            "provenance": provenance,
            "object_id": object_id,
            # "prompt": structured_prompt,
            "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        with open(output_file, "w") as f:
            json.dump(all_transcriptions, f, indent=4)
        return True  # Return success status
    except Exception as e:
        print(f"Error processing {provenance}: {e}")
        return False

# Replace the threading implementation
with ThreadPoolExecutor(max_workers=20) as executor:
    # Create a list to store future-to-path mapping
    future_to_path = {
        executor.submit(process_provenance, provenance, object_id, OUTPUT_DIR, MODELS): provenance 
        for provenance, object_id in zip(PROVENANCES, IDS)
    }
    
    # Monitor completion with better error handling
    for future in tqdm(
        concurrent.futures.as_completed(future_to_path), 
        total=len(PROVENANCES),
        desc="Processing provenances"
    ):
        provenance = future_to_path[future]
        try:
            result = future.result()
            if not result:
                # print(f"Failed to process: {image_path}")
                pass
        except Exception as e:
            # pass
            print(f"Exception while processing {provenance}: {e}")

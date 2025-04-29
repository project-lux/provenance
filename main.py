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
from pydantic import BaseModel, Field, ValidationError
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
OUTPUT_FAILED_DIR = "output_failed"
MODELS = [
    # "google/gemini-1.5-pro-002",
    # "google/gemini-1.5-flash-002"
    "google/gemini-2.5-pro-preview-03-25"
]


df = pd.read_csv("/Users/wjm55/data/provenance/YUAG_provenance.csv")

# Sort by provenance length and get top 50 longest entries
df['provenance_length'] = df['Provenance'].str.len()
df = df.nlargest(500, 'provenance_length')
# Sample 50 random entries from the 500 longest provenances
df = df.sample(n=50, random_state=42)

df = df.drop('provenance_length', axis=1)

PROVENANCES = df["Provenance"].tolist()
IDS = df["ObjectID"].tolist()


os.makedirs(f"{OUTPUT_DIR}", exist_ok=True)
os.makedirs(f"{OUTPUT_FAILED_DIR}", exist_ok=True)


class Actor(BaseModel):
    name: Union[Literal["Unknown"], str]

    # could be list of locations
    location: Union[Literal["Unknown"], str]

    birth_date: Union[Literal["Unknown"], str]
    death_date: Union[Literal["Unknown"], str]

    # change to allow the model to select multiple types
    type: Literal["Person", "Institution", "Collector", "Dealer", "Group", "Unknown", "Building", "Other"]

# Provenance activity
class Movement(BaseModel):
    source: Union[Literal["Unknown"], Actor]
    target: Union[Literal["Unknown"], Actor]
    start_time: Union[Literal["Unknown"], str]
    end_time: Union[Literal["Unknown"], str]

    # might be known but may be unknown - complex
    location: Union[Literal["Unknown"], str]

    confidence: Literal["Certain", "Possible"]

    # Handle Gaps
    method: Union[Literal[*PROVENANCE_METHODS], str]

    change_in_ownership: Union[bool, Literal["Unknown"]]
    change_in_location: Union[bool, Literal["Unknown"]]
    change_in_custody: Union[bool, Literal["Unknown"]]

class Provenance(BaseModel):
    movements: List[Movement]

# Build the schema description
schema_description = Provenance.model_json_schema()
print(schema_description)

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

    output_file = f"{output_dir}/{object_id}.json"
    output_failed_file = f"{OUTPUT_FAILED_DIR}/{object_id}.json"

    # Check existing files
    if ignore_existing and os.path.exists(output_file):
        print(f"Skipping {object_id}: Success output exists.")
        return True
    if ignore_existing and os.path.exists(output_failed_file):
        print(f"Skipping {object_id}: Failed output exists.")
        return False

    max_retries = 3
    successful_transcriptions_for_item = []
    errors_for_item = {}

    for model in models:
        last_error_for_model = None
        success_for_model = False
        for attempt in range(max_retries):
            try:
                print(f"Attempt {attempt + 1}/{max_retries} for {object_id} with model {model}")
                transcription_result = model_call(
                    structured_prompt,
                    client,
                    Provenance,
                    model=model,
                    method="entities",
                    temperature=0.5,
                    max_tokens=8000,
                )
                transcription_result["model"] = model 
                successful_transcriptions_for_item.append(transcription_result)
                last_error_for_model = None
                success_for_model = True
                print(f"Success for {object_id} with model {model} on attempt {attempt + 1}")
                break

            except ValueError as e:
                print(f"Schema validation failed for {object_id}, model {model} (Attempt {attempt + 1}): {e}")
                last_error_for_model = f"ValidationError: {e}"
                if attempt == max_retries - 1:
                    print(f"Max retries reached for {object_id}, model {model}. Model failed.")

            except Exception as e:
                 print(f"Error during model call for {object_id}, model {model} (Attempt {attempt + 1}): {e}")
                 last_error_for_model = f"Exception: {e}"
                 print(f"Non-validation error for {object_id}, model {model}. Model failed.")
                 break

        if not success_for_model and last_error_for_model:
            errors_for_item[model] = last_error_for_model

    if successful_transcriptions_for_item:
        all_transcriptions = {
            "provenance": provenance,
            "output": successful_transcriptions_for_item,
            "object_id": object_id,
            "errors": errors_for_item,
            "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        with open(output_file, "w") as f:
            json.dump(all_transcriptions, f, indent=4)
        print(f"Successfully processed {object_id}. Saved to {output_file}")
        return True
    else:
        print(f"All models failed for {object_id} after retries. Saving error info.")
        error_data = {
            "errors": errors_for_item,
            "provenance": provenance,
            "object_id": object_id,
            "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        with open(output_failed_file, "w") as f:
            json.dump(error_data, f, indent=4)
        return False

with ThreadPoolExecutor(max_workers=20) as executor:
    future_to_provenance = {
        executor.submit(process_provenance, provenance, object_id, OUTPUT_DIR, MODELS, ignore_existing=True): (provenance, object_id)
        for provenance, object_id in zip(PROVENANCES, IDS)
    }
    
    successful_count = 0
    failed_count = 0
    for future in tqdm(
        concurrent.futures.as_completed(future_to_provenance), 
        total=len(PROVENANCES),
        desc="Processing provenances"
    ):
        provenance, object_id = future_to_provenance[future]
        try:
            result = future.result()
            if result:
                successful_count += 1
            else:
                failed_count += 1
        except Exception as e:
            print(f"Exception for {object_id} during future processing: {e}")
            failed_count += 1
            error_data = {
                "error": f"Critical future processing error: {e}",
                "provenance": provenance,
                "object_id": object_id,
                "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            output_critical_fail_file = f"{OUTPUT_FAILED_DIR}/{object_id}_critical.json"
            try:
                 with open(output_critical_fail_file, "w") as f:
                     json.dump(error_data, f, indent=4)
            except Exception as write_e:
                 print(f"Could not write critical failure log for {object_id}: {write_e}")

print(f"\nProcessing Complete. Successful: {successful_count}, Failed: {failed_count}")

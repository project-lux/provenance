import requests
import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import time
from tqdm import tqdm


output_dir = "/Users/wjm55/data/getty"
output_page_dir = "/Users/wjm55/data/getty_page"
# Create output directories
os.makedirs(output_dir, exist_ok=True)
os.makedirs(output_page_dir, exist_ok=True)

start_url = "https://data.getty.edu/museum/collection/activity-stream"

def check_object_ownership(object_url):
    try:
        response = requests.get(object_url)
        if response.status_code == 200:
            object_data = json.loads(response.text)
            if "changed_ownership_through" in object_data:
                object_id = object_url.split('/')[-1]
                with open(f"{output_dir}/ownership_{object_id}.json", 'w') as f:
                    json.dump(object_data, f, indent=2)
                return True
    except Exception as e:
        print(f"Error checking object {object_url}: {str(e)}")
    return False

def get_page_data(page_url):
    try:
        response = requests.get(page_url)
        if response.status_code == 200:
            data = json.loads(response.text)
            # Save the page data
            page_num = page_url.split('/')[-1]
            with open(f"{output_page_dir}/page_{page_num}.json", 'w') as f:
                json.dump(data, f, indent=2)
            
            # Check each object in the page
            if "orderedItems" in data:
                for item in data["orderedItems"]:
                    if "object" in item and "id" in item["object"]:
                        object_url = item["object"]["id"]
                        # Only process URLs containing 'object' in the path
                        if "/object/" in object_url:
                            check_object_ownership(object_url)
    except Exception as e:
        print(f"Error processing {page_url}: {str(e)}")

def main():
    # Get the first and last page numbers
    response = requests.get(start_url)
    data = json.loads(response.text)
    last_page_url = data["last"]["id"]
    last_page_num = int(last_page_url.split('/')[-1])
    
    # Create a list of all page URLs
    page_urls = [f"https://data.getty.edu/museum/collection/activity-stream/page/{i}" 
                for i in range(1, last_page_num + 1)]
    
    # Process pages in parallel using 50 threads with progress bar
    with ThreadPoolExecutor(max_workers=50) as executor:
        list(tqdm(executor.map(get_page_data, page_urls), total=len(page_urls), desc="Downloading pages"))

if __name__ == "__main__":
    main()

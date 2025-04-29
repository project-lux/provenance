import srsly


data_file = "/Users/wjm55/data/chicago-provenance/artworks-with-provenance.jsonl"

data = list(srsly.read_jsonl(data_file))

for i, item in enumerate(data[:100]):
    print(f"Item {i}:")
    print(item["provenance_text"])
    print("\n")
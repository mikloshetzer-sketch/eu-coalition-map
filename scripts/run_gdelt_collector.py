import json
import os
from datetime import datetime

OUTPUT_DIR = "data/events/gdelt"

os.makedirs(OUTPUT_DIR, exist_ok=True)

today = datetime.utcnow().strftime("%Y-%m-%d")
output_file = f"{OUTPUT_DIR}/{today}.jsonl"

events = []

# ide később jön a valódi GDELT lekérés
# most csak teszt adat

sample_event = {
    "layer": "gdelt",
    "source_name": "GDELT",
    "title": "Sample diplomatic interaction",
    "countries": ["FR", "DE"],
    "country_pairs": [["FR", "DE"]],
    "topics": ["diplomacy"],
    "primary_topic": "diplomacy",
    "collected_at": datetime.utcnow().isoformat()
}

events.append(sample_event)

with open(output_file, "w") as f:
    for e in events:
        f.write(json.dumps(e) + "\n")

print("GDELT sample event written.")

# scripts/06_export_mongo.py
from pymongo import MongoClient
import json
from pathlib import Path
import datetime

ROOT = Path(__file__).resolve().parents[1]
out = ROOT / "results" / "mongo_export_sales.json"

client = MongoClient("mongodb://localhost:27017")
docs = client["retail_store"]["sales"].find()

with open(out, "w", encoding="utf-8") as f:
    for d in docs:
        # convert _id and datetime for JSON
        d["_id"] = str(d["_id"])
        for key, val in d.items():
            if isinstance(val, (datetime.datetime, datetime.date)):
                d[key] = val.isoformat()
        f.write(json.dumps(d, ensure_ascii=False) + "\n")

print("saved:", out)

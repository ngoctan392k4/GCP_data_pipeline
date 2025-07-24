import os
from pymongo import MongoClient
import json
from tqdm import tqdm
import logging
from yaml_config import load_config

config = load_config()

def save_product_data_jsonl(data):
    os.makedirs("data", exist_ok=True)
    with open("data/product_data.jsonl", "a", encoding="utf-8") as wf:
        for doc in data:
            json.dump(doc, wf, ensure_ascii=False)
            wf.write("\n")

def download_product():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config["CREDENTIAL"]
    client = MongoClient(config["MONGODB_URL"], serverSelectionTimeoutMS=5)
    database = client["countly"]
    collection = database["product_data"]

    docs = docs = collection.find({}, {"_id": 0})
    batch = []

    for doc in tqdm(docs, total=collection.count_documents({})):
        batch.append(doc)
        if len(batch) == 100:
            save_product_data_jsonl(batch)
            batch.clear()
    if batch:
        save_product_data_jsonl(batch)

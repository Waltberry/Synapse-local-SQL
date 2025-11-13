#!/usr/bin/env python
import pathlib, requests

BASE = pathlib.Path("/workspace")
DEST = BASE / "data" / "product_data"
DEST.mkdir(parents=True, exist_ok=True)

url = "https://raw.githubusercontent.com/MicrosoftLearning/dp-203-azure-data-engineer/master/Allfiles/labs/01/adventureworks/products.csv"
out = DEST / "products.csv"

print(f"Downloading {url} -> {out}")
r = requests.get(url, timeout=60)
r.raise_for_status()
out.write_bytes(r.content)
print("Done.")

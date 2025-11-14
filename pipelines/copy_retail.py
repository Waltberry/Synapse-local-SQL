#!/usr/bin/env python3
import os, urllib.request, pathlib

BASE = pathlib.Path("/workspace/data/lake/RetailDB")
files = {
    BASE/"Customer"/"customer.csv": "https://raw.githubusercontent.com/MicrosoftLearning/dp-203-azure-data-engineer/master/Allfiles/labs/04/data/customer.csv",
    BASE/"Product"/"product.csv":   "https://raw.githubusercontent.com/MicrosoftLearning/dp-203-azure-data-engineer/master/Allfiles/labs/04/data/product.csv",
    BASE/"SalesOrder"/"salesorder.csv": "https://raw.githubusercontent.com/MicrosoftLearning/dp-203-azure-data-engineer/master/Allfiles/labs/04/data/salesorder.csv",
}

for path, url in files.items():
    path.parent.mkdir(parents=True, exist_ok=True)
    print(f"-> downloading {url} -> {path}")
    urllib.request.urlretrieve(url, path)
print("Done.")

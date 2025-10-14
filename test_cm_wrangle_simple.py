#!/usr/bin/env python
"""Test the wrangle logic with new API response."""

from coinmetrics.api_client import CoinMetricsClient
import pandas as pd

# Initialize client
client = CoinMetricsClient()

print("Testing the full workflow...")
print()

# Step 1: Get data from new API
print("Step 1: Fetching data from reference_data_asset_metrics...")
resp = client.reference_data_asset_metrics()
data_resp = resp.to_list()
print(f"Got {len(data_resp)} metrics")
print()

# Step 2: Simulating cm_meta_resp function
print("Step 2: Converting to DataFrame (simulating cm_meta_resp)...")
meta = pd.DataFrame(data_resp)  # store in df
print(f"DataFrame shape before setting index: {meta.shape}")
print(f"Columns: {meta.columns.tolist()}")
print()

meta.set_index(meta.columns[0], inplace=True)  # set index (first column is 'metric')
meta.index.name = 'fields'  # name index col
print(f"DataFrame shape after setting index: {meta.shape}")
print(f"Index name: {meta.index.name}")
print()

print("First few rows:")
print(meta.head())
print()

# Step 3: Check expected values
if 'AdrActCnt' in meta.index:
    print("✓ SUCCESS: Found expected metric 'AdrActCnt'")
    print(f"  Category: {meta.loc['AdrActCnt', 'category']}")
    print(f"  Description: {meta.loc['AdrActCnt', 'description'][:100]}...")
else:
    print("✗ ERROR: 'AdrActCnt' not found in metrics")
    sys.exit(1)

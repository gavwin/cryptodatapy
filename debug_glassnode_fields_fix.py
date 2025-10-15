"""
Debug script to test Glassnode fields API response wrangling.
This tests the fix for gn_fields_info() to handle the new API format.
"""
from cryptodatapy.extract.datarequest import DataRequest
from cryptodatapy.util.datacredentials import DataCredentials
from cryptodatapy.transform.wrangle import WrangleInfo

# Get raw API response
data_cred = DataCredentials()
url = data_cred.glassnode_base_url.replace('/v1/metrics/', '/v1/metadata/') + 'metrics'
params = {'api_key': data_cred.glassnode_api_key}

raw_resp = DataRequest().get_req(url=url, params=params)

print(f"API returned {len(raw_resp)} fields")
print(f"First few items: {raw_resp[:3]}")

# Test the wrangling
result = WrangleInfo(raw_resp).gn_fields_info(as_list=True)
print(f"\nResult type: {type(result)}")
print(f"Result length: {len(result)}")
if len(result) > 0:
    print(f"First 5 fields: {result[:5]}")

    # Check if 'addresses/count' is in there (from the test)
    if any('addresses/count' in field for field in result):
        print("✓ 'addresses/count' found in fields list!")
    else:
        print("✗ 'addresses/count' NOT found in fields list")
        # Show what fields contain 'addresses'
        addr_fields = [f for f in result if 'addresses' in f]
        print(f"Fields containing 'addresses': {addr_fields[:10]}")

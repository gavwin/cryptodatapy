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
print(f"First few raw paths: {raw_resp[:3]}")

# Test the wrangling
result = WrangleInfo(raw_resp).gn_fields_info(as_list=True)
print(f"\nResult type: {type(result)}")
print(f"Result length: {len(result)}")
if len(result) > 0:
    print(f"First 5 cleaned fields: {result[:5]}")

    # Check for specific fields that the tests/code expect
    test_fields = ['addresses/count', 'addresses/active_count', 'transactions/count', 'market/price_usd_ohlc']

    print("\nChecking for expected fields:")
    for field in test_fields:
        if field in result:
            print(f"✓ '{field}' found in fields list!")
        else:
            print(f"✗ '{field}' NOT found in fields list")
            # Try to find similar fields
            similar = [f for f in result if field.split('/')[0] in f or field.split('/')[-1] in f][:5]
            if similar:
                print(f"  Similar fields: {similar}")

    # Show fields containing 'addresses', 'transactions', or 'market'
    print("\nSample fields by category:")
    for category in ['addresses', 'transactions', 'market']:
        cat_fields = [f for f in result if f.startswith(category)][:5]
        print(f"{category}: {cat_fields}")

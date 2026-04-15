#!/usr/bin/env python3
"""Quick diagnostic to isolate the Looker connection issue."""

import json
import os
import ssl
from urllib import error, parse, request

print("=" * 50)
print("  Looker Connection Diagnostic")
print("=" * 50)

# 1. Check env vars
base_url = os.environ.get("LOOKER_BASE_URL", "")
client_id = os.environ.get("LOOKER_CLIENT_ID", "")
client_secret = os.environ.get("LOOKER_CLIENT_SECRET", "")

print(f"\n[1] Environment variables:")
print(f"  LOOKER_BASE_URL    = '{base_url}'")
print(f"  LOOKER_CLIENT_ID   = '{client_id[:4]}...' (length={len(client_id)})")
print(f"  LOOKER_CLIENT_SECRET = '{'*' * min(len(client_secret), 4)}...' (length={len(client_secret)})")

if not base_url:
    print("\n  PROBLEM: LOOKER_BASE_URL is empty. Check your .env file.")
    exit(1)
if not client_id or not client_secret:
    print("\n  PROBLEM: Client credentials are empty. Check your .env file.")
    exit(1)

base_url = base_url.rstrip("/")
ctx = ssl._create_unverified_context()

# 2. Test GET /api/4.0/versions (known working via curl)
print(f"\n[2] GET {base_url}/api/4.0/versions ...")
try:
    req = request.Request(url=f"{base_url}/api/4.0/versions", method="GET")
    with request.urlopen(req, timeout=15, context=ctx) as resp:
        data = json.loads(resp.read().decode())
        print(f"  OK — Looker version: {data.get('looker_release_version', '?')}")
except Exception as e:
    print(f"  FAILED: {e}")
    print("  Cannot reach Looker API at all. Check URL / network.")
    exit(1)

# 3. Test POST /api/4.0/login with urllib (matches instance-connector approach)
print(f"\n[3] POST {base_url}/api/4.0/login (urllib, form-encoded) ...")
payload = parse.urlencode({
    "client_id": client_id,
    "client_secret": client_secret,
}).encode("utf-8")
req = request.Request(
    url=f"{base_url}/api/4.0/login",
    data=payload,
    method="POST",
    headers={"Content-Type": "application/x-www-form-urlencoded"},
)
try:
    with request.urlopen(req, timeout=30, context=ctx) as resp:
        raw = json.loads(resp.read().decode())
        token = raw.get("access_token", "")
        print(f"  OK — got access_token ({len(token)} chars)")
except error.HTTPError as e:
    body = e.read().decode()[:300]
    print(f"  HTTP {e.code}: {body}")
    if e.code == 404:
        print("  Hint: 404 on login usually means bad credentials or the API path is wrong.")
    elif e.code == 401:
        print("  Hint: 401 means credentials are invalid. Check Admin > Users > API3 Keys.")
except Exception as e:
    print(f"  FAILED: {type(e).__name__}: {e}")

# 4. Test POST /api/4.0/login with requests library
print(f"\n[4] POST {base_url}/api/4.0/login (requests library, form-encoded) ...")
try:
    import requests
    r = requests.post(
        f"{base_url}/api/4.0/login",
        data={"client_id": client_id, "client_secret": client_secret},
        verify=False,
        timeout=30,
    )
    print(f"  HTTP {r.status_code}: {r.text[:200]}")
except Exception as e:
    print(f"  FAILED: {type(e).__name__}: {e}")

print("\n" + "=" * 50)
print("  Done")
print("=" * 50)

#!/usr/bin/env python3
"""
Fetch pump.fun launches via Solana RPC.
Run via GitHub Actions every 10 min.
"""
import os
import csv
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from solders.signature import Signature
from solders.pubkey import Pubkey

# ── Config ──────────────────────────────────────────────────────────────
PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
CREATE_DISC = bytes.fromhex("e445a52e51cb9a1d")
DATA_FILE = "data/launches.csv"
MAX_SIGS = 1000
RPC_URL = os.getenv("RPC_URL")

if not RPC_URL:
    raise SystemExit("RPC_URL env var required (set in GitHub Secrets)")

# ── Resilient HTTP Session ──────────────────────────────────────────────
session = requests.Session()
retries = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["POST"],
    raise_on_status=False,
)
session.mount("https://", HTTPAdapter(max_retries=retries))

def rpc(method: str, params: list) -> dict:
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    r = session.post(RPC_URL, json=payload, timeout=15)
    r.raise_for_status()
    data = r.json()
    if "error" in data:
        raise RuntimeError(f"RPC error: {data['error']}")
    return data["result"]

# ── Load Existing Mints ─────────────────────────────────────────────────
existing_mints = set()
if os.path.exists(DATA_FILE):
    with open(DATA_FILE, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            existing_mints.add(row["mint"])

print(f"Loaded {len(existing_mints)} existing mints")

# ── Fetch Recent Signatures ─────────────────────────────────────────────
print("Fetching signatures...")
sigs_resp = rpc("getSignaturesForAddress", [
    PROGRAM_ID,
    {"limit": MAX_SIGS}
])
signatures = [sig["signature"] for sig in sigs_resp]
print(f"Got {len(signatures)} signatures")

# ── Process Each Signature ──────────────────────────────────────────────
new_rows = []
for i, sig in enumerate(signatures):
    if i % 50 == 0:
        print(f"  Progress: {i}/{len(signatures)}")
    
    try:
        tx = rpc("getTransaction", [
            sig,
            {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 1}
        ])
    except Exception as e:
        print(f"  ⚠️  Failed {sig[:8]}...: {e}")
        continue

    if not tx or "meta" not in tx:
        continue

    # Find Create instruction by discriminator in raw data
    create_ix = None
    for ix in tx["transaction"]["message"]["instructions"]:
        # Check parsed instruction (if available)
        parsed = ix.get("parsed")
        if isinstance(parsed, dict) and parsed.get("type") == "create":
            create_ix = ix
            break
        # Fallback: check raw data for discriminator
        data = ix.get("data")
        if isinstance(data, str):
            try:
                if bytes.fromhex(data).startswith(CREATE_DISC):
                    create_ix = ix
                    break
            except Exception:
                pass

    if not create_ix:
        continue

    # Extract mint from postTokenBalances (new token account)
    pre_accounts = {b["accountIndex"] for b in tx["meta"].get("preTokenBalances", [])}
    mint = None
    for b in tx["meta"].get("postTokenBalances", []):
        if b["accountIndex"] not in pre_accounts:
            mint = b.get("mint")
            break

    if not mint or mint in existing_mints:
        continue

    # Creator = fee payer (first account)
    creator = tx["transaction"]["message"]["accountKeys"][0]["pubkey"]
    block_time = tx.get("blockTime", int(time.time()))
    launch_date = time.strftime("%Y-%m-%d", time.gmtime(block_time))

    new_rows.append({
        "mint": mint,
        "creator": creator,
        "timestamp": block_time,
        "signature": sig,
        "launch_date": launch_date,
    })
    existing_mints.add(mint)

    time.sleep(0.1)  # Rate limit: 10 req/s

# ── Append to CSV ───────────────────────────────────────────────────────
if new_rows:
    file_exists = os.path.exists(DATA_FILE)
    with open(DATA_FILE, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["mint", "creator", "timestamp", "signature", "launch_date"])
        if not file_exists:
            writer.writeheader()
        writer.writerows(new_rows)
    print(f"✅ Added {len(new_rows)} new launches")
else:
    print("No new launches found")

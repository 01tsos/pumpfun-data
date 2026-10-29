import os, csv, json, time, requests
from solders.pubkey import Pubkey
from solders.signature import Signature

RPC = os.getenv("RPC_URL", "https://api.mainnet-beta.solana.com")
PROGRAM = Pubkey.from_string("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P")
CREATE_DISC = bytes.fromhex("e445a52e51cb9a1d")
CSV_FILE = "data/launches.csv"

os.makedirs("data", exist_ok=True)

# Load existing mints to avoid duplicates
seen = set()
if os.path.exists(CSV_FILE):
    with open(CSV_FILE) as f:
        for row in csv.DictReader(f):
            seen.add(row["mint"])

def rpc(method, params):
    r = requests.post(RPC, json={"jsonrpc":"2.0","id":1,"method":method,"params":params}, timeout=15)
    r.raise_for_status()
    return r.json()["result"]

# 1. Get recent signatures
sigs = rpc("getSignaturesForAddress", [str(PROGRAM), {"limit": 500}])
print(f"Fetched {len(sigs)} signatures")

new_rows = []
for s in sigs:
    sig = s["signature"]
    if sig in seen:
        continue
    # 2. Get full transaction
    tx = rpc("getTransaction", [sig, {"encoding":"jsonParsed","maxSupportedTransactionVersion":1}])
    if not tx:
        continue
    # 3. Find Create instruction
    for ixg in tx.get("meta",{}).get("innerInstructions",[]):
        for ix in ixg.get("instructions",[]):
            if ix.get("programId") == str(PROGRAM):
                data = ix.get("data","")
                if data and data.startswith("5Z9"):  # base58 of CREATE_DISC
                    # Parse mint from token balances
                    meta = tx["meta"]
                    pre = {b["accountIndex"]:b for b in meta.get("preTokenBalances",[])}
                    for post in meta.get("postTokenBalances",[]):
                        if post["accountIndex"] in pre:
                            continue
                        mint = post["mint"]
                        if mint in seen:
                            continue
                        creator = tx["transaction"]["message"]["accountKeys"][0]
                        ts = tx.get("blockTime", 0)
                        new_rows.append({
                            "mint": mint, "creator": creator, "timestamp": ts,
                            "signature": sig, "launch_date": time.strftime("%Y-%m-%d", time.gmtime(ts))
                        })
                        seen.add(mint)
                        print(f"  New launch: {mint}")
                        break

# 4. Append to CSV
if new_rows:
    file_exists = os.path.exists(CSV_FILE)
    with open(CSV_FILE, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["mint","creator","timestamp","signature","launch_date"])
        if not file_exists:
            w.writeheader()
        w.writerows(new_rows)
    print(f"Added {len(new_rows)} launches")
else:
    print("No new launches")

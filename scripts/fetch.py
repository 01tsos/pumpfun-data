from typing import Optional, List, Tuple
import os, csv, json, time, tempfile, random
from pathlib import Path
import base58, requests

PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
CREATE_DISC = bytes.fromhex("e445a52e51cb9a1d")
EXCLUDED_MINTS = {"So11111111111111111111111111111111111111112","EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v","Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB","mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So","stSOLSo1anaDStStakingD7H4Qw1Qx8X9M9gGzK","JUPyiwrYFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN","7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj","27G8MtK7VtTcCHkpASjSDdkWWYfoqT6ggEuKidVJidD4"}

DATA_FILE = Path("data/launches.csv")
STATE_FILE = Path("data/collector_state.json")
CSV_FIELDS = ["mint","creator","timestamp","launch_date","signature","name","symbol","uri","source"]

# Three free endpoints - rotate to spread load
RPC_URLS = [
    os.getenv("QUICKNODE_URL", "https://wiser-hardworking-crater.solana-mainnet.quiknode.pro/f4b4c699cfe58c8f7ccd0f812308f5b039564bd3/"),
    "https://solana-rpc.publicnode.com",
    "https://solana-mainnet.g.alchemy.com/v2/alch_HDCBngOY92gdH30NFx5b3",
]
endpoint_idx = 0

MAX_SIGNATURE_PAGES = 2
PAGE_SIZE = 200
REQUEST_DELAY = 10.0

session = requests.Session()

def atomic_write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, delete=False) as tmp:
        json.dump(value, tmp, indent=2); tmp.write("\n")
        os.replace(tmp.name, path)

def load_state():
    if not STATE_FILE.exists(): return {}
    try: return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except: return {}

def load_existing_rows():
    if not DATA_FILE.exists(): return [], set(), set()
    with DATA_FILE.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    return rows, {r.get("mint") for r in rows if r.get("mint")}, {r.get("signature") for r in rows if r.get("signature")}

def write_rows(rows):
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", newline="", encoding="utf-8", dir=DATA_FILE.parent, delete=False) as tmp:
        w = csv.DictWriter(tmp, fieldnames=CSV_FIELDS, extrasaction="ignore")
        w.writeheader()
        for r in rows: w.writerow({f: r.get(f, "") for f in CSV_FIELDS})
        os.replace(tmp.name, DATA_FILE)

def is_ratelimit(text):
    t = text.lower()
    return any(m in t for m in ["429","rate limit","too many requests","credits","throttle","quota"])

def is_permanent(text):
    t = text.lower()
    return any(m in t for m in ["403","forbidden","401","unauthorized","404","not found"])

def get_rpc_url():
    global endpoint_idx
    url = RPC_URLS[endpoint_idx % len(RPC_URLS)]
    endpoint_idx += 1
    return url

def rpc(method, params):
    global endpoint_idx
    last_err = None
    for attempt in range(8):
        url = get_rpc_url()
        try:
            resp = session.post(url, json={"jsonrpc":"2.0","id":1,"method":method,"params":params}, timeout=30)
            if resp.status_code == 429:
                wait = min(60, 2 ** min(attempt, 5)) + 2.0
                print(f"Rate limited ({url}); waiting {wait:.1f}s")
                time.sleep(wait); continue
            if resp.status_code in (403,401):
                print(f"Auth error ({url}), skipping")
                continue
            resp.raise_for_status()
            payload = resp.json()
            if "error" in payload:
                msg = str(payload["error"])
                if is_ratelimit(msg):
                    wait = min(60, 2 ** min(attempt, 5)) + 2.0
                    print(f"RPC rate limit ({url}); waiting {wait:.1f}s")
                    time.sleep(wait); continue
                if is_permanent(msg): raise RuntimeError(msg)
                raise RuntimeError(msg)
            return payload.get("result")
        except requests.exceptions.JSONDecodeError:
            print(f"Invalid JSON ({url}), retrying"); continue
        except Exception as e:
            last_err = e; print(f"RPC failure ({url}): {e}")
            if attempt < 7: time.sleep(min(30, 3 + attempt))
    raise RuntimeError(f"All RPC attempts failed for {method}: {last_err}")

def fetch_signatures(cursor):
    collected, before, reached = [], None, cursor is None
    for p in range(MAX_SIGNATURE_PAGES):
        cfg = {"limit": PAGE_SIZE, "commitment": "confirmed"}
        if before: cfg["before"] = before
        if cursor: cfg["until"] = cursor
        print(f"Fetching signature page {p+1}")
        page = rpc("getSignaturesForAddress", [PROGRAM_ID, cfg])
        if not page: reached = True; break
        collected.extend(page)
        if cursor and any(i.get("signature") == cursor for i in page): reached = True; break
        before = page[-1].get("signature")
        if len(page) < PAGE_SIZE: reached = True; break
        time.sleep(0.5)
    return collected, reached

def account_keys(tx): return [i.get("pubkey","") if isinstance(i,dict) else str(i) for i in tx.get("transaction",{}).get("message",{}).get("accountKeys",[])]

def instr_prog_id(ix, keys):
    if ix.get("programId"): return ix["programId"]
    idx = ix.get("programIdIndex")
    if isinstance(idx,int) and idx < len(keys): return keys[idx]
    return None

def decode_ix(ix):
    d = ix.get("data")
    if not isinstance(d,str): return None
    try: return base58.b58decode(d)
    except: return None

def find_create(tx):
    keys = account_keys(tx)
    ixs = list(tx.get("transaction",{}).get("message",{}).get("instructions",[]))
    for g in tx.get("meta",{}).get("innerInstructions",[]): ixs.extend(g.get("instructions",[]))
    for ix in ixs:
        if instr_prog_id(ix, keys) != PROGRAM_ID: continue
        dec = decode_ix(ix)
        if dec and dec[:8] == CREATE_DISC: return ix, dec
        if isinstance(ix.get("parsed"),dict) and ix["parsed"].get("type") == "create": return ix, b""
    return None, None

def read_borsh(data, off):
    if off+4 > len(data): return None, off
    l = int.from_bytes(data[off:off+4], "little"); off += 4
    if l < 0 or off+l > len(data): return None, off
    return data[off:off+l].decode("utf-8","replace"), off+l

def parse_specs(dec):
    if not dec or len(dec) <= 8: return "","",""
    off = 8
    name, off = read_borsh(dec, off)
    if name is None: return "","",""
    sym, off = read_borsh(dec, off)
    if sym is None: return name,"",""
    uri, _ = read_borsh(dec, off)
    if uri is None: return name,sym,""
    return name,sym,uri

def mint_from_balances(tx):
    meta = tx.get("meta",{})
    pre = {i.get("mint") for i in meta.get("preTokenBalances",[]) if i.get("mint")}
    post = [i.get("mint") for i in meta.get("postTokenBalances",[]) if i.get("mint")]
    for m in post:
        if m not in pre and m not in EXCLUDED_MINTS: return m
    return None

def extract_launch(tx, sig):
    ix, dec = find_create(tx)
    if not ix: return None
    mint = mint_from_balances(tx)
    if not mint: return None
    bt = tx.get("blockTime")
    if not bt: return None
    n,s,u = parse_specs(dec or b"")
    return {"mint": mint, "creator": account_keys(tx)[0] if account_keys(tx) else "", "timestamp": bt,
            "launch_date": time.strftime("%Y-%m-%d", time.gmtime(bt)), "signature": sig,
            "name": n, "symbol": s, "uri": u, "source": "solana_rpc"}

def main():
    print(f"Using {len(RPC_URLS)} endpoints (rotating)")
    rows, mints, sigs = load_existing_rows()
    state = load_state()
    cursor = state.get("last_seen_signature")
    print(f"Existing: {len(mints)}, cursor: {cursor or 'none'}")
    sigs_list, _ = fetch_signatures(cursor)
    if not sigs_list: print("No signatures"); return
    newest = sigs_list[0].get("signature")
    new_rows, fails, seen = [], 0, set()
    start = time.time()
    for idx, item in enumerate(reversed(sigs_list), 1):
        if time.time() - start > 1800: print("Time limit"); break
        sig = item.get("signature")
        if not sig or sig in sigs: continue
        print(f"Tx {idx}/{len(sigs_list)}")
        try:
            tx = rpc("getTransaction", [sig, {"encoding":"jsonParsed","commitment":"confirmed","maxSupportedTransactionVersion":1}])
        except Exception as e:
            fails += 1; print(f"Fail {sig[:12]}: {e}"); continue
        if not tx: continue
        launch = extract_launch(tx, sig)
        if not launch: continue
        m = launch["mint"]
        if m in mints or m in seen: continue
        new_rows.append(launch); seen.add(m)
        print(f"NEW: {m} {launch.get('name','')} {launch.get('symbol','')}")
        time.sleep(REQUEST_DELAY)
    if new_rows: write_rows(rows + new_rows); print(f"Added {len(new_rows)}")
    else: print("No new launches")
    if fails == 0:
        atomic_write_json(STATE_FILE, {"last_seen_signature": newest, "updated_at": int(time.time()),
            "signatures_scanned": len(sigs_list), "launches_added": len(new_rows)})
        print(f"Cursor -> {newest}")
    else: print("Cursor held (failures)")
    print(f"Scanned: {len(sigs_list)}, Added: {len(new_rows)}, Fails: {fails}")

if __name__ == "__main__": main()

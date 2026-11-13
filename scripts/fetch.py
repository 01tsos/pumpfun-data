from typing import Optional, List, Tuple
#!/usr/bin/env python3
"""
Fetch pump.fun launches via Helius RPC (fast, free tier).
Run via GitHub Actions every 5 min.
"""
import os
import csv
import json
import time
import tempfile
from pathlib import Path

import base58
import requests

# ── Config ──────────────────────────────────────────────────────────────
PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
CREATE_DISC = bytes.fromhex("e445a52e51cb9a1d")

EXCLUDED_MINTS = {
    "So11111111111111111111111111111111111111112",  # wrapped SOL
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
    "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So",   # mSOL
    "stSOLSo1anaDStStakingD7H4Qw1Qx8X9M9gGzK",       # stSOL
    "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",    # JUP
    "7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj",   # stUSDC
    "27G8MtK7VtTcCHkpASjSDdkWWYfoqT6ggEuKidVJidD4",   # jitoSOL
}

DATA_FILE = Path("data/launches.csv")
STATE_FILE = Path("data/collector_state.json")

CSV_FIELDS = [
    "mint", "creator", "timestamp", "launch_date",
    "signature", "name", "symbol", "uri", "source",
]

# Helius RPC (uses free tier: 1M req/mo)
HELIUS_RPC = f"https://mainnet.helius-rpc.com/?api-key={os.getenv('HELIUS_API_KEY')}"

MAX_SIGNATURE_PAGES = int(os.getenv("MAX_SIGNATURE_PAGES", "5"))
PAGE_SIZE = 1000
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "0.02"))

session = requests.Session()
session.headers.update({"User-Agent": "pumpfun-collector/1.0"})


def atomic_write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, delete=False
    ) as tmp:
        json.dump(value, tmp, indent=2)
        tmp.write("\n")
        temp_name = tmp.name
    os.replace(temp_name, path)


def load_state() -> dict:
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def load_existing_rows() -> tuple[List[dict], set[str], set[str]]:
    if not DATA_FILE.exists():
        return [], set(), set()
    with DATA_FILE.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    existing_mints = {row.get("mint") for row in rows if row.get("mint")}
    existing_signatures = {row.get("signature") for row in rows if row.get("signature")}
    return rows, existing_mints, existing_signatures


def write_rows(rows: List[dict]) -> None:
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", newline="", encoding="utf-8", dir=DATA_FILE.parent, delete=False
    ) as tmp:
        writer = csv.DictWriter(tmp, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in CSV_FIELDS})
        temp_name = tmp.name
    os.replace(temp_name, DATA_FILE)


def rpc(method: str, params: list):
    """Helius RPC with simple retry."""
    for attempt in range(3):
        try:
            response = session.post(
                HELIUS_RPC,
                json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                timeout=30,
            )
            if response.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            response.raise_for_status()
            payload = response.json()
            if "error" in payload:
                raise RuntimeError(payload["error"])
            return payload.get("result")
        except Exception as e:
            if attempt == 2:
                raise
            time.sleep(1)
    raise RuntimeError(f"Helius RPC failed for {method}")


def fetch_signatures(cursor: Optional[str]) -> Tuple[List[dict], bool]:
    """
    Return signatures newer than the previous cursor.
    Signatures are returned newest first.
    The boolean indicates whether the old cursor was reached.
    """
    collected = []
    before = None
    cursor_reached = cursor is None
    for page_number in range(MAX_SIGNATURE_PAGES):
        config = {"limit": PAGE_SIZE, "commitment": "confirmed"}
        if before:
            config["before"] = before
        # Don't pass cursor in config; we check manually to get only newer sigs
        print(f"Fetching signature page {page_number + 1}")
        page = rpc("getSignaturesForAddress", [PROGRAM_ID, config])
        if not page:
            cursor_reached = True
            break

        if cursor:
            # Find cursor in this page
            cursor_idx = None
            for i, item in enumerate(page):
                if item.get("signature") == cursor:
                    cursor_idx = i
                    break
            if cursor_idx is not None:
                # Take only signatures before cursor (newer than cursor)
                collected.extend(page[:cursor_idx])
                cursor_reached = True
                break
            else:
                collected.extend(page)
        else:
            collected.extend(page)

        before = page[-1].get("signature")
        if len(page) < PAGE_SIZE:
            cursor_reached = True
            break
    # If cursor provided but not found after all pages, all collected are newer than cursor
    if cursor and not cursor_reached:
        cursor_reached = True

    return collected, cursor_reached


def account_keys(tx: dict) -> List[str]:
    keys = []
    for item in tx.get("transaction", {}).get("message", {}).get("accountKeys", []):
        if isinstance(item, dict):
            keys.append(item.get("pubkey", ""))
        else:
            keys.append(str(item))
    return keys


def instruction_program_id(ix: dict, keys: List[str]) -> Optional[str]:
    if ix.get("programId"):
        return ix["programId"]
    index = ix.get("programIdIndex")
    if isinstance(index, int) and index < len(keys):
        return keys[index]
    return None


def decode_instruction_data(ix: dict) -> Optional[bytes]:
    data = ix.get("data")
    if not isinstance(data, str):
        return None
    try:
        return base58.b58decode(data)
    except Exception:
        return None


def find_create_instruction(tx: dict):
    keys = account_keys(tx)
    instructions = []
    message_instructions = tx.get("transaction", {}).get("message", {}).get("instructions", [])
    instructions.extend(message_instructions)
    meta = tx.get("meta", {})
    inner_instructions = meta.get("innerInstructions") or []
    for group in inner_instructions:
        instructions.extend(group.get("instructions", []))
    for ix in instructions:
        if instruction_program_id(ix, keys) != PROGRAM_ID:
            continue
        decoded = decode_instruction_data(ix)
        if decoded and decoded[:8] == CREATE_DISC:
            return ix, decoded
        parsed = ix.get("parsed")
        if isinstance(parsed, dict) and parsed.get("type") == "create":
            return ix, b""
    return None, None


def read_borsh_string(data: bytes, offset: int):
    if offset + 4 > len(data):
        return None, offset
    length = int.from_bytes(data[offset:offset + 4], byteorder="little")
    offset += 4
    if length < 0 or offset + length > len(data):
        return None, offset
    value = data[offset:offset + length].decode("utf-8", errors="replace")
    return value, offset + length


def parse_specs(decoded: bytes) -> Tuple[str, str, str]:
    if not decoded or len(decoded) <= 8:
        return "", "", ""
    offset = 8
    name, offset = read_borsh_string(decoded, offset)
    if name is None:
        return "", "", ""
    symbol, offset = read_borsh_string(decoded, offset)
    if symbol is None:
        return name, "", ""
    uri, offset = read_borsh_string(decoded, offset)
    if uri is None:
        return name, symbol, ""
    return name, symbol, uri


def mint_from_balances(tx: dict) -> Optional[str]:
    meta = tx.get("meta", {})
    pre_mints = {item.get("mint") for item in meta.get("preTokenBalances", []) if item.get("mint")}
    post_mints = [item.get("mint") for item in meta.get("postTokenBalances", []) if item.get("mint")]
    for mint in post_mints:
        if mint not in pre_mints and mint not in EXCLUDED_MINTS:
            return mint
    return None


def extract_launch(tx: dict, signature: str) -> Optional[dict]:
    ix, decoded = find_create_instruction(tx)
    if ix is None:
        return None
    mint = mint_from_balances(tx)
    if not mint:
        return None
    block_time = tx.get("blockTime")
    if not block_time:
        return None
    creator_keys = account_keys(tx)
    creator = creator_keys[0] if creator_keys else ""
    name, symbol, uri = parse_specs(decoded or b"")
    return {
        "mint": mint,
        "creator": creator,
        "timestamp": block_time,
        "launch_date": time.strftime("%Y-%m-%d", time.gmtime(block_time)),
        "signature": signature,
        "name": name,
        "symbol": symbol,
        "uri": uri,
        "source": "solana_rpc",
    }


def main() -> None:
    print(f"Using Helius RPC")
    existing_rows, existing_mints, existing_signatures = load_existing_rows()
    state = load_state()
    old_cursor = state.get("last_seen_signature")
    print(f"Existing launches: {len(existing_mints)}")
    print(f"Previous cursor: {old_cursor or 'none'}")
    signatures, cursor_reached = fetch_signatures(old_cursor)
    if not signatures:
        print("No signatures returned")
        return
    newest_signature = signatures[0].get("signature")
    
    to_process = [
        item for item in reversed(signatures)
        if item.get("signature") and item["signature"] not in existing_signatures
    ]
    print(f"Processing {len(to_process)} new signatures...")
    
    new_rows = []
    failed_transactions = 0
    seen_batch_mints = set()
    processed = 0
    start_time = time.time()
    MAX_RUNTIME = 18 * 60  # 18 min (Helius is fast)
    
    for index, item in enumerate(to_process, start=1):
        if time.time() - start_time > MAX_RUNTIME:
            print(f"Runtime limit reached, stopping early")
            cursor_reached = False
            break
        
        signature = item["signature"]
        processed += 1
        
        if processed % 100 == 0:
            print(f"  Progress: {processed}/{len(to_process)}")
        
        try:
            tx = rpc("getTransaction", [
                signature,
                {"encoding": "jsonParsed", "commitment": "confirmed", "maxSupportedTransactionVersion": 1}
            ])
        except Exception as exc:
            failed_transactions += 1
            print(f"Transaction failed {signature[:12]}...: {exc}")
            continue
        
        if not tx:
            continue
        
        launch = extract_launch(tx, signature)
        if not launch:
            continue
        
        mint = launch["mint"]
        if mint in existing_mints or mint in seen_batch_mints:
            continue
        
        new_rows.append(launch)
        seen_batch_mints.add(mint)
        print(f"NEW LAUNCH: {mint} {launch.get('name', '')} {launch.get('symbol', '')}")
        
        time.sleep(REQUEST_DELAY)
    
    if new_rows:
        write_rows(existing_rows + new_rows)
        print(f"Added {len(new_rows)} launches")
    else:
        print("No new launches found")
    
    if cursor_reached and failed_transactions == 0:
        atomic_write_json(STATE_FILE, {
            "last_seen_signature": newest_signature,
            "updated_at": int(time.time()),
            "signatures_scanned": len(signatures),
            "launches_added": len(new_rows),
        })
        print(f"Cursor advanced to {newest_signature}")
    else:
        print("Cursor was not advanced. The next run will retry this range.")
    
    print(f"Signatures scanned: {len(signatures)}")
    print(f"Launches added: {len(new_rows)}")
    print(f"Transaction failures: {failed_transactions}")
    print(f"Time elapsed: {time.time() - start_time:.0f}s")


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Fetch pump.fun launches via Solana RPC.
Run via GitHub Actions every 5 min.
"""
import os
import csv
import json
import time
import random
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

# Free endpoints - NO API KEY, verified working
DEFAULT_RPC_URLS = [
    "https://api.mainnet-beta.solana.com",
    "https://solana-rpc.publicnode.com",
    "https://rpc.ankr.com/solana",
    "https://solana.drpc.org",
]

RPC_URLS = [
    url.strip()
    for url in (
        os.getenv("RPC_URLS")
        or os.getenv("RPC_URL")
        or ",".join(DEFAULT_RPC_URLS)
    ).split(",")
    if url.strip()
]

# More signatures, less delay = better coverage within time limit
MAX_SIGNATURE_PAGES = int(os.getenv("MAX_SIGNATURE_PAGES", "5"))
PAGE_SIZE = 1000
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "0.08"))  # ~12 req/s

session = requests.Session()
session.headers.update({"User-Agent": "pumpfun-collector/1.0"})
endpoint_index = 0


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


def load_existing_rows() -> tuple[list[dict], set[str], set[str]]:
    if not DATA_FILE.exists():
        return [], set(), set()
    with DATA_FILE.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    existing_mints = {row.get("mint") for row in rows if row.get("mint")}
    existing_signatures = {row.get("signature") for row in rows if row.get("signature")}
    return rows, existing_mints, existing_signatures


def write_rows(rows: list[dict]) -> None:
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


def is_rate_limit_error(text: str) -> bool:
    text = text.lower()
    return any(marker in text for marker in ["429", "rate limit", "too many requests", "credits", "throttle", "quota"])


def is_permanent_error(text: str) -> bool:
    text = text.lower()
    return any(marker in text for marker in ["403", "forbidden", "401", "unauthorized", "404", "not found"])


def rpc(method: str, params: list):
    global endpoint_index
    last_error = None
    max_attempts = max(4, len(RPC_URLS) * 2)
    for attempt in range(max_attempts):
        url = RPC_URLS[endpoint_index % len(RPC_URLS)]
        endpoint_index += 1
        try:
            response = session.post(
                url,
                json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                timeout=20,
            )
            if response.status_code == 429:
                wait = min(20, 2 ** min(attempt, 4))
                wait += random.uniform(0, 1)
                print(f"Rate limited by {url}; waiting {wait:.1f}s")
                time.sleep(wait)
                continue
            if response.status_code in (403, 401):
                print(f"Endpoint {url} returned {response.status_code}, skipping")
                continue
            response.raise_for_status()
            payload = response.json()
            if "error" in payload:
                message = str(payload["error"])
                if is_rate_limit_error(message):
                    wait = min(20, 2 ** min(attempt, 4))
                    wait += random.uniform(0, 1)
                    print(f"RPC rate limit; waiting {wait:.1f}s")
                    time.sleep(wait)
                    continue
                if is_permanent_error(message):
                    print(f"Permanent error from {url}: {message}")
                    continue
                raise RuntimeError(message)
            return payload.get("result")
        except requests.exceptions.JSONDecodeError:
            print(f"Invalid JSON from {url}, skipping")
            continue
        except Exception as exc:
            last_error = exc
            print(f"RPC failure on {url}: {exc}")
            if attempt < max_attempts - 1:
                time.sleep(min(10, 1 + attempt))
    raise RuntimeError(f"All RPC attempts failed for {method}: {last_error}")


def fetch_signatures(cursor: str | None) -> tuple[list[dict], bool]:
    collected = []
    before = None
    cursor_reached = cursor is None
    for page_number in range(MAX_SIGNATURE_PAGES):
        config = {"limit": PAGE_SIZE, "commitment": "confirmed"}
        if before:
            config["before"] = before
        if cursor:
            config["until"] = cursor
        print(f"Fetching signature page {page_number + 1}")
        page = rpc("getSignaturesForAddress", [PROGRAM_ID, config])
        if not page:
            cursor_reached = True
            break
        collected.extend(page)
        if cursor and any(item.get("signature") == cursor for item in page):
            cursor_reached = True
            break
        before = page[-1].get("signature")
        if len(page) < PAGE_SIZE:
            cursor_reached = True
            break
    return collected, cursor_reached


def account_keys(tx: dict) -> list[str]:
    keys = []
    for item in tx.get("transaction", {}).get("message", {}).get("accountKeys", []):
        if isinstance(item, dict):
            keys.append(item.get("pubkey", ""))
        else:
            keys.append(str(item))
    return keys


def instruction_program_id(ix: dict, keys: list[str]) -> str | None:
    if ix.get("programId"):
        return ix["programId"]
    index = ix.get("programIdIndex")
    if isinstance(index, int) and index < len(keys):
        return keys[index]
    return None


def decode_instruction_data(ix: dict) -> bytes | None:
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
    # FIX: innerInstructions can be None
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


def parse_specs(decoded: bytes) -> tuple[str, str, str]:
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


def mint_from_balances(tx: dict) -> str | None:
    meta = tx.get("meta", {})
    pre_mints = {item.get("mint") for item in meta.get("preTokenBalances", []) if item.get("mint")}
    post_mints = [item.get("mint") for item in meta.get("postTokenBalances", []) if item.get("mint")]
    for mint in post_mints:
        if mint not in pre_mints and mint not in EXCLUDED_MINTS:
            return mint
    return None


def extract_launch(tx: dict, signature: str) -> dict | None:
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
    print(f"Using RPC endpoints: {len(RPC_URLS)}")
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
    new_rows = []
    failed_transactions = 0
    seen_batch_mints = set()
    start_time = time.time()
    MAX_RUNTIME = 38 * 60  # 38 min (workflow timeout is 40)
    for index, item in enumerate(reversed(signatures), start=1):
        if time.time() - start_time > MAX_RUNTIME:
            print(f"Runtime limit reached ({MAX_RUNTIME}s), stopping early")
            cursor_reached = False
            break
        signature = item.get("signature")
        if not signature:
            continue
        if signature in existing_signatures:
            continue
        print(f"Processing transaction {index}/{len(signatures)}")
        try:
            tx = rpc("getTransaction", [signature, {"encoding": "jsonParsed", "commitment": "confirmed", "maxSupportedTransactionVersion": 1}])
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
        atomic_write_json(STATE_FILE, {"last_seen_signature": newest_signature, "updated_at": int(time.time()), "signatures_scanned": len(signatures), "launches_added": len(new_rows)})
        print(f"Cursor advanced to {newest_signature}")
    else:
        print("Cursor was not advanced. The next run will retry this range.")
    print(f"Signatures scanned: {len(signatures)}")
    print(f"Launches added: {len(new_rows)}")
    print(f"Transaction failures: {failed_transactions}")


if __name__ == "__main__":
    main()

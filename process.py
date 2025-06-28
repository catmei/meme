import os
import json
import csv
import datetime
from web3 import Web3
from dotenv import load_dotenv
from web3.exceptions import MismatchedABI
from eth_abi.exceptions import InsufficientDataBytes

# Load environment variables from .env file (if ALCHEMY_API_KEY is still managed here for processing)
load_dotenv()

# Initialize Web3 with Alchemy URL from environment variable
ALCHEMY_API_KEY = os.getenv("ALCHEMY_API_KEY")
if not ALCHEMY_API_KEY:
    print("Warning: ALCHEMY_API_KEY not found in environment. Web3 functionality in process.py may fail.")
    web3 = None
else:
    ALCHEMY_URL = f"https://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}"
    web3 = Web3(Web3.HTTPProvider(ALCHEMY_URL))
    if not web3.is_connected():
        print(f"Warning: Failed to connect to Alchemy at {ALCHEMY_URL}. Web3 functionality in process.py may fail.")

# Uniswap V3 swap event signature
# Swap(address,address,int256,int256,uint160,uint128,int24)
SWAP_EVENT_SIGNATURE = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"

# Minimal ABIs
ERC20_ABI = [
    {"constant": True, "inputs": [], "name": "decimals", "outputs": [{"name": "", "type": "uint8"}], "payable": False, "stateMutability": "view", "type": "function"},
    {"constant": True, "inputs": [], "name": "symbol", "outputs": [{"name": "", "type": "string"}], "payable": False, "stateMutability": "view", "type": "function"}
]
UNI_V3_POOL_ABI = [
    {"inputs": [], "name": "token0", "outputs": [{"internalType": "address", "name": "", "type": "address"}], "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "token1", "outputs": [{"internalType": "address", "name": "", "type": "address"}], "stateMutability": "view", "type": "function"}
]

# CSV Cache configuration
POOL_INFO_CSV_FILE = "pool_info.csv"
CSV_HEADERS = ["pool_address", "token0_address", "token0_decimals", "token0_symbol", "token1_address", "token1_decimals", "token1_symbol"]

# Swap Events data configuration
SWAP_EVENTS_DATA_HEADERS = ["block", "timestamp", "amount0", "amount1", "sqrtPriceX96", "pool_address", "log_index", "tx_hash", "network", "alchemy_createdAt", "server_createdAt"]

# Cache for pool token info {pool_address: {token0: {...}, token1: {...}}}
POOL_INFO_CACHE = {}



def get_token_info(token_address):
    """Fetches decimals and symbol for a given token address."""
    if not web3 or not web3.is_connected():
        print("  Web3 not available or not connected. Cannot fetch token info.")
        return {"address": token_address, "decimals": 18, "symbol": token_address[:6] + '...'}

    try:
        checksum_address = Web3.to_checksum_address(token_address)
        token_contract = web3.eth.contract(address=checksum_address, abi=ERC20_ABI)
        decimals = token_contract.functions.decimals().call()
        try:
            symbol = token_contract.functions.symbol().call()
        except Exception:
            symbol = checksum_address[:6] + '...' # Shortened address
        return {"address": checksum_address, "decimals": decimals, "symbol": symbol}
    except Exception as e:
        print(f"  Error fetching info for token {token_address}: {e}")
        return {"address": token_address, "decimals": 18, "symbol": token_address[:6] + '...'}

def get_pool_info(pool_address):
    """Gets token0 and token1 info for a pool, using cache if available (memory then CSV)."""
    if not web3 or not web3.is_connected():
        print("  Web3 not available or not connected. Cannot fetch pool info.")
        return None

    try:
        checksum_pool_address = Web3.to_checksum_address(pool_address)
    except ValueError:
        print(f"  Invalid pool address format provided: {pool_address}")
        return None

    # 1. Check in-memory cache
    if checksum_pool_address in POOL_INFO_CACHE:
        # print(f"  Found pool info for {checksum_pool_address} in memory cache.") # Optional: for debugging
        return POOL_INFO_CACHE[checksum_pool_address]

    # 2. Check CSV file
    if os.path.exists(POOL_INFO_CSV_FILE):
        try:
            with open(POOL_INFO_CSV_FILE, mode='r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                # Ensure headers are present and match exactly before reading
                if not reader.fieldnames or list(reader.fieldnames) != CSV_HEADERS:
                    print(f"  Warning: CSV file '{POOL_INFO_CSV_FILE}' has missing or mismatched headers. Expected {CSV_HEADERS}, got {reader.fieldnames}. Will attempt Web3 fetch if needed.")
                else:
                    for row in reader:
                        if row.get("pool_address") == checksum_pool_address:
                            print(f"  Found pool info for {checksum_pool_address} in {POOL_INFO_CSV_FILE}")
                            try:
                                pool_info = {
                                    "token0": {"address": row["token0_address"], "decimals": int(row["token0_decimals"]), "symbol": row["token0_symbol"]},
                                    "token1": {"address": row["token1_address"], "decimals": int(row["token1_decimals"]), "symbol": row["token1_symbol"]}
                                }
                                POOL_INFO_CACHE[checksum_pool_address] = pool_info # Update in-memory cache
                                return pool_info
                            except (KeyError, ValueError) as e:
                                print(f"  Error parsing row for {checksum_pool_address} from CSV: {e}. Data: {row}")
                                # Break or continue, depending on desired behavior if a row is corrupt
                                break 
        except Exception as e:
            print(f"  Error reading {POOL_INFO_CSV_FILE}: {e}. Proceeding to fetch from Web3.")

    # 3. Fetch from Web3 (original logic)
    print(f"  Fetching info for pool via Web3: {checksum_pool_address}")
    try:
        pool_contract = web3.eth.contract(address=checksum_pool_address, abi=UNI_V3_POOL_ABI)
        token0_address = pool_contract.functions.token0().call()
        token1_address = pool_contract.functions.token1().call()

        token0_info = get_token_info(token0_address)
        token1_info = get_token_info(token1_address)

        # Ensure token_info is valid before caching and writing to CSV
        if not (
           (token0_info and token1_info) and
           all(k in token0_info for k in ["address", "decimals", "symbol"]) and
           all(k in token1_info for k in ["address", "decimals", "symbol"])
        ):
            print(f"  Failed to fetch complete or valid token info for pool {checksum_pool_address}. Cannot cache.")
            return None

        pool_info = {
            "token0": token0_info,
            "token1": token1_info
        }
        POOL_INFO_CACHE[checksum_pool_address] = pool_info # Update in-memory cache

        # 4. Save to CSV
        try:
            file_exists_and_not_empty = os.path.exists(POOL_INFO_CSV_FILE) and os.path.getsize(POOL_INFO_CSV_FILE) > 0
            write_header = not file_exists_and_not_empty

            with open(POOL_INFO_CSV_FILE, mode='a', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=CSV_HEADERS)
                if write_header:
                    writer.writeheader()
                
                # Ensure all keys are present before writing, using defaults from get_token_info if necessary
                writer.writerow({
                    "pool_address": checksum_pool_address,
                    "token0_address": token0_info.get("address", "N/A"),
                    "token0_decimals": token0_info.get("decimals", 18),
                    "token0_symbol": token0_info.get("symbol", "ERR"),
                    "token1_address": token1_info.get("address", "N/A"),
                    "token1_decimals": token1_info.get("decimals", 18),
                    "token1_symbol": token1_info.get("symbol", "ERR")
                })
            print(f"  Saved pool info for {checksum_pool_address} to {POOL_INFO_CSV_FILE}")
        except Exception as e:
            print(f"  Error saving pool info to {POOL_INFO_CSV_FILE}: {e}")

        return pool_info
    except Exception as e:
        print(f"  Error fetching info for pool {checksum_pool_address} via Web3: {e}")
        return None

SWAP_EVENT_DATA_TYPES = ['int256', 'int256', 'uint160', 'uint128', 'int24']

def process_swap_event(data):
    """Process a Uniswap swap event, decode data, calculate price, and return data rows."""
    if not web3 or not web3.is_connected():
        print("Web3 not available or not connected. Cannot process swap event.")
        return [] # Return empty list if Web3 is not available

    event_payload = data.get("event", {})
    event_data = event_payload.get("data", {})
    block_info = event_data.get("block", {})
    
    block_number = block_info.get("number")
    block_timestamp = block_info.get("timestamp")
    logs = block_info.get("logs", [])

    # Extract top-level info for CSV, common to all logs in this webhook
    network = event_payload.get("network", "N/A")
    alchemy_createdAt = data.get("createdAt", "N/A")

    if not logs:
        print("No logs found in the block data.")
        return [] # Return empty list if no logs

    processed_rows = [] # Initialize list to store rows

    for log_data in logs:
        transaction_hash = log_data.get("transaction", {}).get("hash", "N/A")
        log_index = log_data.get("index", "N/A")
        raw_pool_address = log_data.get("account", {}).get("address", "N/A")

        print(f"\n--- Swap Event Detected ---")
        print(f"  Pool: {raw_pool_address}")
        print(f"  Tx Hash: {transaction_hash}")
        print(f"  Block: {block_number if block_number is not None else 'N/A'}")
        print(f"  Timestamp: {block_timestamp if block_timestamp is not None else 'N/A'}")
        print(f"  Log Index: {log_index}")

        pool_info = None
        token0 = None
        token1 = None
        
        final_pool_address_for_csv = "N/A"
        if raw_pool_address and raw_pool_address != "N/A":
            try:
                # Attempt to checksum early for consistent use, also for get_pool_info
                checksummed_pool_address = Web3.to_checksum_address(raw_pool_address)
                final_pool_address_for_csv = checksummed_pool_address
                pool_info = get_pool_info(checksummed_pool_address) # Use checksummed here
            except ValueError:
                print(f"  Warning: Malformed pool address '{raw_pool_address}'. Using raw address for processing.")
                final_pool_address_for_csv = raw_pool_address # Store as is for CSV
                pool_info = get_pool_info(raw_pool_address) # Attempt with raw if checksum failed
        else: # raw_pool_address is "N/A" or None
            print("  Pool address is N/A. Skipping pool info fetch.")


        # Initialize values for this specific log
        amount0_processed = "N/A"
        amount1_processed = "N/A"
        sqrtPriceX96_processed = "N/A"
        can_process_data = False

        if pool_info:
            token0 = pool_info.get("token0")
            token1 = pool_info.get("token1")
            if token0 and token1: # Ensure token info is complete
                data_hex = log_data.get("data")
                if data_hex and data_hex != "0x":
                    try:
                        data_bytes = bytes.fromhex(data_hex[2:])
                        decoded_data = web3.codec.decode(SWAP_EVENT_DATA_TYPES, data_bytes)

                        amount0_raw = decoded_data[0]
                        amount1_raw = decoded_data[1]
                        sqrtPriceX96_raw = decoded_data[2]
                        
                        print(f"  Amount0 Change (raw): {amount0_raw}")
                        print(f"  Amount1 Change (raw): {amount1_raw}")
                        print(f"  Decimals T0: {token0['decimals']}, T1: {token1['decimals']}")

                        amount0_adj = amount0_raw / (10**token0['decimals'])
                        amount1_adj = amount1_raw / (10**token1['decimals'])

                        print(f"  Amount0 Change ({token0['symbol']}): {amount0_adj:.{token0['decimals']}f}")
                        print(f"  Amount1 Change ({token1['symbol']}): {amount1_adj:.{token1['decimals']}f}")
                        print(f"  Raw sqrtPriceX96: {sqrtPriceX96_raw}")
                        
                        # Prepare processed data (use adjusted values, formatted)
                        amount0_processed = f"{amount0_adj:.{token0['decimals']}f}"
                        amount1_processed = f"{amount1_adj:.{token1['decimals']}f}"
                        sqrtPriceX96_processed = sqrtPriceX96_raw
                        can_process_data = True

                        if sqrtPriceX96_raw > 0:
                            price_1_0 = ((sqrtPriceX96_raw / (2**96))**2) / (10**(token1['decimals'] - token0['decimals']))
                            price_0_1 = 1 / price_1_0 if price_1_0 != 0 else 0
                            print(f"\n  Calculated Price:")
                            print(f"    1 {token1['symbol']} = {price_1_0:.{max(8,token0['decimals'])}f} {token0['symbol']}")
                            print(f"    1 {token0['symbol']} = {price_0_1:.{max(8,token1['decimals'])}f} {token1['symbol']}")
                        else:
                            print("\n  Price calculation skipped (sqrtPriceX96 is zero)")

                    except (MismatchedABI, InsufficientDataBytes, ValueError, Exception) as e:
                        print(f"\n  Error decoding event data or calculating price: {e}")
                        print(f"    Raw data: {data_hex}")
                else:
                    print("\n  No data field found in log.")
            else:
                print("  Token information (token0 or token1) is missing from pool_info. Skipping detailed processing.")
        else: # pool_info was not retrieved or is None
            print("  Could not retrieve valid pool info. Skipping detailed processing for this log.")
            # can_process_data remains False

        # After processing the log, add to results if data is valid
        if can_process_data:
            row_data = {
                "block": block_number if block_number is not None else "N/A",
                "timestamp": block_timestamp if block_timestamp is not None else "N/A",
                "amount0": amount0_processed,
                "amount1": amount1_processed,
                "sqrtPriceX96": sqrtPriceX96_processed,
                "pool_address": final_pool_address_for_csv, # Already checksummed or handled if malformed
                "log_index": log_index,
                "tx_hash": transaction_hash,
                "network": network,
                "alchemy_createdAt": alchemy_createdAt,
                "server_createdAt": datetime.datetime.now(datetime.timezone.utc).isoformat()
            }
            # Append the prepared row to the list
            processed_rows.append(row_data)
        else:
            # This 'else' branch might be mostly covered by previous print statements,
            # but can serve as a fallback or for specific debugging if needed.
            # For example, if can_process_data is False due to incomplete token0/token1 from pool_info
            if not (pool_info and token0 and token1 and data_hex and data_hex != "0x"):
                 # Error messages for these conditions are already printed earlier in the logic flow
                 pass # No additional generic message needed here as specific errors were logged
            else:
                 # This case implies pool_info, tokens, data_hex were present, but decoding failed (already logged)
                 # Or some other logic prevented can_process_data from being true
                 print("  Skipping data processing for this log due to processing issues (check previous errors).")


        print("--- End Swap Event (process.py) ---")
    
    return processed_rows # Return the list of processed rows

if __name__ == '__main__':
    # Example Usage (for testing process.py directly)
    print("Testing process.py functions...")
    if web3 and web3.is_connected():
        print(f"Connected to Web3 provider: {web3.provider}")
        # Replace with a known Uniswap V3 pool address for testing
        test_pool_address = "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640" # USDC/ETH 0.05%
        print(f"\nTesting get_pool_info for: {test_pool_address}")
        info = get_pool_info(test_pool_address)
        if info:
            print(f"Pool Info: {json.dumps(info, indent=2)}")

        # Sample data from the provided JSON
        sample_data = {
        "webhookId": "wh_pyb17707pay24x1k",
        "id": "whevt_4eth6rakhsen33oh",
        "createdAt": "2025-05-15T04:28:16.043Z",
        "type": "GRAPHQL",
        "event": {
            "data": {
            "block": {
                "hash": "0x37813ad0454d2b8b9431ec39c1fc23aa0924e25000425f067a9e71e2ccfefeea",
                "number": 22486139,
                "timestamp": 1747283291,
                "logs": [
                {
                    "data": "0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffffaebd961000000000000000000000000000000000000000000000000007502135eeeeea30000000000000000000000000000000000004cc76d370228d32a925f65eb5b110000000000000000000000000000000000000000000000007c744829aefa552a0000000000000000000000000000000000000000000000000000000000030464",
                    "topics": [
                    "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",
                    "0x00000000000000000000000066a9893cc07d91d95644aedd05d03f95e1dba8af",
                    "0x00000000000000000000000013272aadbc7573594e98191b6502023938907fff"
                    ],
                    "index": 397,
                    "account": {
                    "address": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"
                    },
                    "transaction": {
                    "hash": "0xfac2c92365bf282c819666024f69ec200f2f8a24e04ca89e40409cd158acf0f1",
                    "from": {
                        "address": "0xe9ee00dcafd1b5ca6883df1e15281b2a3cd51419"
                    },
                    "to": {
                        "address": "0x66a9893cc07d91d95644aedd05d03f95e1dba8af"
                    },
                    "value": "0x1065d1a9519e761",
                    "gas": 591407,
                    "gasPrice": "0x1d4289c4",
                    "status": 1
                    }
                },  
                {
                    "data": "0xfffffffffffffffffffffffffffffffffffffffffffffffffffffffffe8287c0000000000000000000000000000000000000000000000000002254a75eba35fa0000000000000000000000000000000000004cc76d7d97373edfbb2d350ff7c70000000000000000000000000000000000000000000000007c744829aefa552a0000000000000000000000000000000000000000000000000000000000030464",
                    "topics": [
                    "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",
                    "0x0000000000000000000000003fc91a3afd70395cd496c647d5a6cc9d4b2b7fad",
                    "0x0000000000000000000000003fc91a3afd70395cd496c647d5a6cc9d4b2b7fad"
                    ],
                    "index": 431,
                    "account": {
                    "address": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"
                    },
                    "transaction": {
                    "hash": "0x94e62c76e49bbaf8f9594c7e5a6fe2685dba92a2ad09d1b03aeb9ae3239573eb",
                    "from": {
                        "address": "0x39cf6e0ba4c4530735616e1ee7ff5fbcb726fbd2"
                    },
                    "to": {
                        "address": "0x1fa57f879417e029ef57d7ce915b0aa56a507c31"
                    },
                    "value": "0x22b74772ba3efe",
                    "gas": 641134,
                    "gasPrice": "0x351a0dc4",
                    "status": 1
                    }
                }
                ]
            }
            },
            "sequenceNumber": "10000000145105416001",
            "network": "ETH_MAINNET"
        }
        }

        print("Testing process_swap_event with data from JSON:")
        list_of_event_rows = process_swap_event(sample_data)

        if list_of_event_rows:
            print(f"Processed {len(list_of_event_rows)} event(s).")
            for row_data in list_of_event_rows:
                print(f"Row data: {row_data}")
        else:
            print("No event data was processed.")
    else:
        print("Web3 is not connected. Cannot run full tests.") 
import os
import json
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
    """Gets token0 and token1 info for a pool, using cache if available."""
    if not web3 or not web3.is_connected():
        print("  Web3 not available or not connected. Cannot fetch pool info.")
        return None

    checksum_pool_address = Web3.to_checksum_address(pool_address)
    if checksum_pool_address in POOL_INFO_CACHE:
        return POOL_INFO_CACHE[checksum_pool_address]

    print(f"  Fetching info for pool: {checksum_pool_address}")
    try:
        pool_contract = web3.eth.contract(address=checksum_pool_address, abi=UNI_V3_POOL_ABI)
        token0_address = pool_contract.functions.token0().call()
        token1_address = pool_contract.functions.token1().call()

        token0_info = get_token_info(token0_address)
        token1_info = get_token_info(token1_address)

        pool_info = {
            "token0": token0_info,
            "token1": token1_info
        }
        POOL_INFO_CACHE[checksum_pool_address] = pool_info
        return pool_info
    except Exception as e:
        print(f"  Error fetching info for pool {checksum_pool_address}: {e}")
        return None

SWAP_EVENT_DATA_TYPES = ['int256', 'int256', 'uint160', 'uint128', 'int24']

def process_swap_event(log_data, block_number, block_timestamp):
    """Process a Uniswap swap event, decode data, and calculate price."""
    if not web3 or not web3.is_connected():
        print("Web3 not available or not connected. Cannot process swap event.")
        return

    transaction_hash = log_data.get("transaction", {}).get("hash", "N/A")
    log_index = log_data.get("index", "N/A")
    pool_address_raw = log_data.get("account", {}).get("address", "N/A")

    pool_address = "N/A"
    if pool_address_raw != "N/A":
        try:
            pool_address = Web3.to_checksum_address(pool_address_raw)
        except ValueError:
            print(f"Warning: Invalid pool address format: {pool_address_raw}")

    print(f"\n--- Swap Event Detected (in process.py) ---")
    print(f"  Pool: {pool_address}")
    print(f"  Tx Hash: {transaction_hash}")
    print(f"  Block: {block_number}") # Expecting already converted block_number if passed
    print(f"  Timestamp: {block_timestamp}") # Expecting already converted block_timestamp if passed
    print(f"  Log Index: {log_index}")

    pool_info = None
    if pool_address != "N/A":
        pool_info = get_pool_info(pool_address)

    if not pool_info:
        print("  Could not retrieve pool info. Skipping detailed processing.")
        print("--- End Swap Event (process.py) ---")
        return

    token0 = pool_info["token0"]
    token1 = pool_info["token1"]

    topics = log_data.get("topics", [])
    if len(topics) >= 3:
        try:
            sender = Web3.to_checksum_address("0x" + topics[1][-40:])
            recipient = Web3.to_checksum_address("0x" + topics[2][-40:])
            print(f"  Sender: {sender}")
            print(f"  Recipient: {recipient}")
        except ValueError as e:
            print(f"  Error decoding sender/recipient address: {e}")
    else:
        print("  Warning: Not enough topics to decode sender/recipient.")

    data_hex = log_data.get("data")
    if data_hex and data_hex != "0x":
        try:
            data_bytes = bytes.fromhex(data_hex[2:])
            decoded_data = web3.codec.decode(SWAP_EVENT_DATA_TYPES, data_bytes)

            amount0 = decoded_data[0]
            amount1 = decoded_data[1]
            sqrtPriceX96 = decoded_data[2]
            # liquidity = decoded_data[3]
            # tick = decoded_data[4]

            print(f"  Amount0 Change (raw): {amount0}")
            print(f"  Amount1 Change (raw): {amount1}")
            print(f"  Decimals T0: {token0['decimals']}, T1: {token1['decimals']}")

            amount0_adj = amount0 / (10**token0['decimals'])
            amount1_adj = amount1 / (10**token1['decimals'])

            print(f"  Amount0 Change ({token0['symbol']}): {amount0_adj:.{token0['decimals']}f}")
            print(f"  Amount1 Change ({token1['symbol']}): {amount1_adj:.{token1['decimals']}f}")
            # print(f"  Liquidity: {liquidity}")
            # print(f"  Tick: {tick}")
            print(f"  Raw sqrtPriceX96: {sqrtPriceX96}")

            if sqrtPriceX96 > 0:
                price_1_0 = ((sqrtPriceX96 / (2**96))**2) / (10**(token1['decimals'] - token0['decimals']))
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

    print("--- End Swap Event (process.py) ---")

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
        sample_log_data_from_json = {
            "data": "0x0000000000000000000000000000000000000000000000000000001b96be31a1fffffffffffffffffffffffffffffffffffffffffffffffd8c867a34dc503e3e0000000000000000000000000000000000004c4fcf68dbecd80000000000000000000000000000000000000000000000000000000000000065c81de63715acc000000000000000000000000000000000000000000000000000000000000303e9",
            "topics": [
                "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",
                "0x0000000000000000000000005dc62cea20b0e7c3607adcc61a885ff9369dbc60",
                "0x0000000000000000000000005dc62cea20b0e7c3607adcc61a885ff9369dbc60"
            ],
            "index": 2,
            "account": {
                "address": "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"
            },
            "transaction": {
                "hash": "0xbc49d4fe3454023eeb0fa442b4f9f2806702b563437908d47c225dc0fc325315",
                "from": {
                    "address": "0x2f61d0de31c5bb5025a6d67c09468fd228562db9"
                },
                "to": {
                    "address": "0x5dc62cea20b0e7c3607adcc61a885ff9369dbc60"
                },
                "value": "0x0",
                "gas": 257535,
                "gasPrice": "0xb1764bcb1",
                "status": 1
            }
        }
        block_number_from_json = 22480141
        block_timestamp_from_json = 1747210343

        # Convert block_number and block_timestamp to hex as expected by the function
        # (assuming the function expects hex strings based on the original commented out example "123456_hex")
        # If they are expected as integers, this conversion can be removed.
        hex_block_number = hex(block_number_from_json)
        hex_block_timestamp = hex(block_timestamp_from_json)

        print("Testing process_swap_event with data from JSON:")
        process_swap_event(sample_log_data_from_json, hex_block_number, hex_block_timestamp)
    else:
        print("Web3 is not connected. Cannot run full tests.") 
import json
import os
from flask import Flask, request, jsonify
from web3 import Web3
from dotenv import load_dotenv
from web3.exceptions import MismatchedABI
from eth_abi.exceptions import InsufficientDataBytes

# Load environment variables from .env file
load_dotenv()

# Initialize Flask app
app = Flask(__name__)

# Initialize Web3 with Alchemy URL from environment variable
ALCHEMY_API_KEY = os.getenv("ALCHEMY_API_KEY")
ALCHEMY_URL = f"https://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}"
web3 = Web3(Web3.HTTPProvider(ALCHEMY_URL))

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
    try:
        token_contract = web3.eth.contract(address=token_address, abi=ERC20_ABI)
        decimals = token_contract.functions.decimals().call()
        try:
            symbol = token_contract.functions.symbol().call()
        except Exception:
            # Fallback if symbol() fails or doesn't exist
            symbol = token_address[:6] + '...' # Shortened address
        return {"address": token_address, "decimals": decimals, "symbol": symbol}
    except Exception as e:
        print(f"  Error fetching info for token {token_address}: {e}")
        return {"address": token_address, "decimals": 18, "symbol": token_address[:6] + '...'} # Default fallback

def get_pool_info(pool_address):
    """Gets token0 and token1 info for a pool, using cache if available."""
    if pool_address in POOL_INFO_CACHE:
        return POOL_INFO_CACHE[pool_address]

    print(f"  Fetching info for pool: {pool_address}")
    try:
        pool_contract = web3.eth.contract(address=pool_address, abi=UNI_V3_POOL_ABI)
        token0_address = pool_contract.functions.token0().call()
        token1_address = pool_contract.functions.token1().call()

        token0_info = get_token_info(token0_address)
        token1_info = get_token_info(token1_address)

        pool_info = {
            "token0": token0_info,
            "token1": token1_info
        }
        POOL_INFO_CACHE[pool_address] = pool_info # Store in cache
        return pool_info

    except Exception as e:
        print(f"  Error fetching info for pool {pool_address}: {e}")
        # Return default structure or None, depending on how you want to handle errors
        return None

# Define the non-indexed types for the Swap event
# amount0, amount1, sqrtPriceX96, liquidity, tick
SWAP_EVENT_DATA_TYPES = ['int256', 'int256', 'uint160', 'uint128', 'int24']

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.json
    # print(f"Received webhook data: {json.dumps(data, indent=2)}") # Keep commented unless debugging

    try:
        event_data = data.get("event", {})
        block_data = event_data.get("data", {}).get("block", {})
        logs = block_data.get("logs", [])

        if not logs:
            print("No logs found in webhook data.")
            return jsonify({"status": "success", "message": "No logs found"}), 200

        block_number = block_data.get("number", "N/A")
        block_timestamp = block_data.get("timestamp", "N/A")
        print(f"Processing Block: {block_number} Timestamp: {block_timestamp}")

        swap_found = False
        for log in logs:
            # Check if this specific log is a swap event
            if log and isinstance(log, dict) and "topics" in log and log["topics"] and log["topics"][0] == SWAP_EVENT_SIGNATURE:
                swap_found = True
                process_swap_event(log, block_number, block_timestamp)

        if not swap_found:
            print(f"No Swap events found in block {block_number}.")

        return jsonify({"status": "success"}), 200

    except Exception as e:
        print(f"Error processing webhook: {str(e)}")
        # Be careful logging full data in production if it contains sensitive info
        # print(f"Errored data: {json.dumps(data)}")
        return jsonify({"status": "error", "message": str(e)}), 500


def process_swap_event(log_data, block_number, block_timestamp):
    """Process a Uniswap swap event, decode data, and calculate price."""
    # print(f"Processing swap event: {json.dumps(log_data, indent=2)}")

    transaction_hash = log_data.get("transaction", {}).get("hash", "N/A")
    log_index = log_data.get("index", "N/A")
    pool_address_raw = log_data.get("account", {}).get("address", "N/A")

    pool_address = "N/A"
    if pool_address_raw != "N/A":
        try:
            pool_address = Web3.to_checksum_address(pool_address_raw)
        except ValueError:
            print(f"Warning: Invalid pool address format: {pool_address_raw}")
            # Optionally handle this error more gracefully if needed

    print(f"\n--- Swap Event Detected ---")
    print(f"  Pool: {pool_address}")
    print(f"  Tx Hash: {transaction_hash}")
    print(f"  Block: {block_number}")
    print(f"  Timestamp: {block_timestamp}")
    print(f"  Log Index: {log_index}")

    # Get dynamic pool info (token addresses, decimals, symbols)
    pool_info = None
    if pool_address != "N/A":
        pool_info = get_pool_info(pool_address)

    if not pool_info:
        print("  Could not retrieve pool info. Skipping detailed processing.")
        print("--- End Swap Event ---")
        return

    token0 = pool_info["token0"]
    token1 = pool_info["token1"]

    # Topics: sig, sender, recipient
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

    # Decode non-indexed parameters
    data_hex = log_data.get("data")
    if data_hex and data_hex != "0x":
        try:
            data_bytes = bytes.fromhex(data_hex[2:])
            decoded_data = web3.codec.decode(SWAP_EVENT_DATA_TYPES, data_bytes)

            amount0 = decoded_data[0]
            amount1 = decoded_data[1]
            sqrtPriceX96 = decoded_data[2]
            liquidity = decoded_data[3]
            tick = decoded_data[4]

            # Use fetched info for display
            print(f"  Amount0 Change: {amount0 / (10**token0['decimals']):.{token0['decimals']}f} {token0['symbol']}")
            print(f"  Amount1 Change: {amount1 / (10**token1['decimals']):.{token1['decimals']}f} {token1['symbol']}")
            print(f"  Liquidity: {liquidity}")
            print(f"  Tick: {tick}")
            print(f"  Raw sqrtPriceX96: {sqrtPriceX96}")

            # Calculate Price using fetched decimals
            if sqrtPriceX96 > 0:
                # Price of Token1 in terms of Token0
                price_1_0 = ((sqrtPriceX96 / (2**96))**2) / (10**(token1['decimals'] - token0['decimals']))
                # Price of Token0 in terms of Token1
                price_0_1 = 1 / price_1_0

                print(f"\n  Calculated Price:")
                print(f"    1 {token1['symbol']} = {price_1_0:.{token0['decimals']}f} {token0['symbol']}")
                print(f"    1 {token0['symbol']} = {price_0_1:.{token1['decimals']}f} {token1['symbol']}")
            else:
                 print("\n  Price calculation skipped (sqrtPriceX96 is zero)")

        except (MismatchedABI, InsufficientDataBytes, ValueError, Exception) as e:
            print(f"\n  Error decoding event data or calculating price: {e}")
            print(f"    Raw data: {data_hex}")
    else:
        print("\n  No data field found in log.")

    print("--- End Swap Event ---")


if __name__ == "__main__":
    print(f"Web3 connected: {web3.is_connected()}")
    print("Starting webhook server...")
    app.run(host="0.0.0.0", port=5000, debug=True) 
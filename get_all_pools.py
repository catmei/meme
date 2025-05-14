import os
import time
from web3 import Web3
from dotenv import load_dotenv

# --- Configuration ---
load_dotenv()
ALCHEMY_API_KEY = os.getenv("ALCHEMY_API_KEY")
if not ALCHEMY_API_KEY:
    raise ValueError("ALCHEMY_API_KEY not found in environment variables. Please ensure it is set in your .env file.")

ALCHEMY_URL = f"https://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}"
FACTORY_ADDRESS = "0x1F98431c8aD98523631AE4a59f267346ea31F984"
# Block where the V3 Factory was deployed (approximate)
# FACTORY_DEPLOYMENT_BLOCK = 12369621 # No longer needed for start block
# How many blocks to query at a time (adjust based on provider limits/performance)
BLOCK_CHUNK_SIZE = 5000
# Estimate blocks per month (30 days * 24 hours * 60 mins * 60 secs / ~12 secs/block)
BLOCKS_PER_MONTH_ESTIMATE = 216000

# Minimal ABI for the PoolCreated event
FACTORY_ABI = [
    {
        "anonymous": False,
        "inputs": [
            {"indexed": True, "internalType": "address", "name": "token0", "type": "address"},
            {"indexed": True, "internalType": "address", "name": "token1", "type": "address"},
            {"indexed": True, "internalType": "uint24", "name": "fee", "type": "uint24"},
            {"indexed": False, "internalType": "int24", "name": "tickSpacing", "type": "int24"},
            {"indexed": False, "internalType": "address", "name": "pool", "type": "address"}
        ],
        "name": "PoolCreated",
        "type": "event"
    }
]
# --- End Configuration ---

def main():
    print(f"Connecting to {ALCHEMY_URL}...")
    web3 = Web3(Web3.HTTPProvider(ALCHEMY_URL))

    if not web3.is_connected():
        print("Error: Failed to connect to Ethereum node.")
        return

    print("Successfully connected.")
    factory_contract = web3.eth.contract(address=FACTORY_ADDRESS, abi=FACTORY_ABI)
    # Get the event object - we'll use it for processing logs
    pool_created_event = factory_contract.events.PoolCreated
    # Calculate the event signature hash (Keccak-256 of the canonical signature)
    event_signature_text = "PoolCreated(address,address,uint24,int24,address)"
    event_signature_hash = web3.keccak(text=event_signature_text).hex()
    # Ensure 0x prefix for the Ethereum JSON-RPC API
    if not event_signature_hash.startswith('0x'):
        event_signature_hash = '0x' + event_signature_hash
    
    latest_block = web3.eth.block_number

    # Calculate approximate start block for the last month
    start_block = max(0, latest_block - BLOCKS_PER_MONTH_ESTIMATE)

    print(f"Uniswap V3 Factory: {FACTORY_ADDRESS}")
    print(f"Latest block: {latest_block}")
    print(f"Scanning pools created since approximately block {start_block} (last ~1 month)...")
    print(f"Scanning in chunks of {BLOCK_CHUNK_SIZE} blocks...")

    MAX_RETRIES = 2 # Maximum number of retries per chunk
    found_pools = set()

    current_scan_block = start_block
    while current_scan_block <= latest_block:
        end_block = min(current_scan_block + BLOCK_CHUNK_SIZE - 1, latest_block)
        print(f"  Scanning blocks {current_scan_block} to {end_block}...")
        
        retry_count = 0
        processed_successfully = False # Flag to check if the try block completed
        
        while retry_count <= MAX_RETRIES and not processed_successfully:
            try:
                # Use web3.eth.get_logs directly
                raw_logs = web3.eth.get_logs({
                    "fromBlock": current_scan_block,
                    "toBlock": end_block,
                    "address": FACTORY_ADDRESS,
                    "topics": [event_signature_hash]
                })

                if raw_logs:
                    print(f"    Found {len(raw_logs)} potential PoolCreated logs in this range.")
                    for raw_log in raw_logs:
                        try:
                            processed_log = pool_created_event().process_log(raw_log)
                            pool_address = processed_log['args']['pool']
                            if pool_address not in found_pools:
                                print(f"      New Pool Found: {pool_address}")
                                found_pools.add(pool_address)
                        except Exception as log_e:
                            print(f"      Error processing individual log: {log_e}")
                            # Decide if you want to stop the whole chunk on a single log error
                            # For now, just prints the error and continues with the next log
                else:
                    print("    No PoolCreated logs found in this range.")

                # If we reached here, the try block succeeded for this chunk
                processed_successfully = True
                time.sleep(0.1) # Small delay after success

            except Exception as e:
                retry_count += 1
                print(f"  Error querying/processing blocks {current_scan_block}-{end_block} (Attempt {retry_count}/{MAX_RETRIES}): {e}")
                if retry_count > MAX_RETRIES:
                    print(f"  Max retries exceeded for blocks {current_scan_block}-{end_block}. Skipping chunk.")
                    # No need to sleep here, just break the inner loop and move to the next chunk
                    break # Exit the inner retry loop
                else:
                    print(f"  Retrying in 5 seconds...")
                    time.sleep(5)
                    # Continue the inner loop to retry

        # Move to the next chunk regardless of whether the last one succeeded or was skipped
        current_scan_block = end_block + 1 

    print(f"\nFinished scanning.")
    print(f"Total unique pools found in the last month: {len(found_pools)}")

    # Optional: Save pools to a file
    # with open("uniswap_v3_pools.txt", "w") as f:
    #     for pool in sorted(list(found_pools)):
    #         f.write(pool + "\n")
    # print("Pool addresses saved to uniswap_v3_pools.txt")

if __name__ == "__main__":
    main() 


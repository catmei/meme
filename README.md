# Uniswap Swap Event Webhook Server

A simple webhook server that processes Uniswap swap events.

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure your Alchemy API key:
Create a `.env` file in the root directory with the following content:
```
ALCHEMY_URL=https://eth-mainnet.g.alchemy.com/v2/YOUR_ALCHEMY_API_KEY
```
Replace `YOUR_ALCHEMY_API_KEY` with your actual Alchemy API key.

## Running the Server

Start the webhook server:
```bash
python app.py
```

The server will run on `http://localhost:5000`.

## Testing

Use the included test script to simulate a webhook call:
```bash
python test_webhook.py
```

## Usage with External Services

To use this webhook with services like SimpleHash (as shown in the screenshot):

1. Deploy this server to a publicly accessible URL
2. Set up your webhook in the external service to point to your server's `/webhook` endpoint
3. Configure the appropriate filters to track Uniswap swap events

## Webhook Format

The server expects webhook data in the following format:
```json
{
  "logs": [
    {
      "topics": ["<swap_event_signature>", ...],
      "transactionHash": "0x...",
      "blockNumber": "0x..."
    }
  ]
}
```

## Note

This is a simple test server and not intended for production use without further security and error handling. 
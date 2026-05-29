import os
from py_clob_client.client import ClobClient
from dotenv import load_dotenv

load_dotenv()

host = "https://clob.polymarket.com"
key = os.getenv("PRIVATE_KEY")
chain_id = 137
proxy = os.getenv("RELAYER_API_KEY_ADDRESS")

client = ClobClient(host, key=key, chain_id=chain_id, signature_type=3, funder=proxy)
creds = client.create_or_derive_api_key()
print("Derived creds:", creds)

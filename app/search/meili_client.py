from meilisearch import Client
import os

MEILI_URL = os.getenv("MEILI_URL", "http://meili:7700")
MEILI_KEY = os.getenv("MEILI_MASTER_KEY", None)

client = Client(MEILI_URL, MEILI_KEY)

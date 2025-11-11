from pymilvus import connections, utility
import os

# Connect to Milvus
host = os.getenv("MILVUS_HOST", "localhost")
port = int(os.getenv("MILVUS_PORT", "19530"))
alias = os.getenv("MILVUS_ALIAS", "default")

print(f"Connecting to Milvus at {host}:{port}...")
try:
    connections.connect(
        alias=alias,
        host=host,
        port=port
    )
    print("Connected successfully!")
except Exception as e:
    print(f"Failed to connect to Milvus: {e}")
    print("\nTroubleshooting:")
    print("1. Ensure Milvus is running: docker ps | grep milvus")
    print("2. Check docker-compose.yml for Milvus configuration")
    print("3. Wait for Milvus to fully start (may take 30-60 seconds)")
    exit(1)

# List collections before drop
print("\nCollections before drop:", utility.list_collections())

# Drop collections
print("\nDropping collections...")
for name in ["diagnosis_items", "lab_result_items", "microbiology_items", "prescription_items"]:
    try:
        if utility.has_collection(name):
            utility.drop_collection(name)
            print(f"✓ Dropped: {name}")
        else:
            print(f"- Collection {name} does not exist (skipped)")
    except Exception as e:
        print(f"✗ Failed {name}: {e}")

# List collections after drop
print("\nCollections after drop:", utility.list_collections())

# Disconnect
connections.disconnect(alias)
print("\nDisconnected from Milvus")

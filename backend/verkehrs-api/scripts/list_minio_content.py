
import os
import argparse
from minio import Minio
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_minio_client():
    minio_endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    secure = False
    
    print(f"Connecting to MinIO at {minio_endpoint}...")
    
    return Minio(
        minio_endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure
    )

def list_content(client):
    try:
        buckets = client.list_buckets()
        print(f"Found {len(buckets)} buckets.")
        
        for bucket in buckets:
            print(f"\nBucket: {bucket.name} (Created: {bucket.creation_date})")
            print("-" * 50)
            
            try:
                objects = client.list_objects(bucket.name, recursive=True)
                count = 0
                for obj in objects:
                    print(f" - {obj.object_name} ({obj.size} bytes) [Last Modified: {obj.last_modified}]")
                    count += 1
                
                if count == 0:
                    print(" (Empty bucket)")
                else:
                    print(f" Total: {count} objects")
                    
            except Exception as e:
                print(f" Error listing objects in bucket '{bucket.name}': {e}")
                
    except Exception as e:
        print(f"Failed to list buckets: {e}")

def download_object(client, bucket_name, object_name, local_path):
    try:
        print(f"Downloading '{object_name}' from bucket '{bucket_name}' to '{local_path}'...")
        client.fget_object(bucket_name, object_name, local_path)
        print("Download successful.")
    except Exception as e:
        print(f"Failed to download object: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Manage MinIO content.")
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # List command
    list_parser = subparsers.add_parser("list", help="List all buckets and objects")
    
    # Download command
    dl_parser = subparsers.add_parser("download", help="Download an object")
    dl_parser.add_argument("bucket", help="Name of the bucket")
    dl_parser.add_argument("object", help="Name of the object to download")
    dl_parser.add_argument("local_path", help="Local path to save the file")
    
    args = parser.parse_args()
    
    client = get_minio_client()
    
    if args.command == "download":
        download_object(client, args.bucket, args.object, args.local_path)
    else:
        # Default to list if no command or 'list' is specified
        list_content(client)

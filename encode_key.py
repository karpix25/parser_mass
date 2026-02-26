#!/usr/bin/env python3
import base64
import sys
import os

def main():
    if len(sys.argv) < 2:
        print("Usage: python encode_key.py <path_to_credentials.json>")
        sys.exit(1)
        
    file_path = sys.argv[1]
    
    if not os.path.exists(file_path):
        print(f"Error: File '{file_path}' not found.")
        sys.exit(1)
        
    try:
        with open(file_path, "rb") as f:
            file_content = f.read()
            
        encoded = base64.b64encode(file_content).decode("utf-8")
        
        print("\n=== BASE64 ENCODED CREDENTIALS ===")
        print("Copy the string below and paste it into your .env file as:")
        print("GOOGLE_CREDENTIALS_B64=...\n")
        print(encoded)
        print("\n==================================\n")
        
    except Exception as e:
        print(f"Error reading or encoding file: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

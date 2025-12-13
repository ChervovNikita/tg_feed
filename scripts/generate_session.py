#!/usr/bin/env python3
"""Script to generate Pyrogram session string."""
import sys
from pyrogram import Client


def main():
    print("=" * 50)
    print("Pyrogram Session String Generator")
    print("=" * 50)
    print()
    print("You need API_ID and API_HASH from https://my.telegram.org")
    print()
    
    api_id = input("Enter API_ID: ").strip()
    api_hash = input("Enter API_HASH: ").strip()
    
    if not api_id or not api_hash:
        print("Error: API_ID and API_HASH are required")
        sys.exit(1)
    
    try:
        api_id = int(api_id)
    except ValueError:
        print("Error: API_ID must be a number")
        sys.exit(1)
    
    print()
    print("Starting authentication...")
    print("You will receive a code in Telegram.")
    print()
    
    with Client("session_generator", api_id=api_id, api_hash=api_hash) as app:
        session_string = app.export_session_string()
        
        print()
        print("=" * 50)
        print("SUCCESS! Your session string:")
        print("=" * 50)
        print()
        print(session_string)
        print()
        print("=" * 50)
        print("Add this to your .env file as SESSION_STRING")
        print("=" * 50)


if __name__ == "__main__":
    main()

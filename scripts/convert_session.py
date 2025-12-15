"""
Convert Pyrogram .session file to session string.

Usage:
    python scripts/convert_session.py path/to/your.session
    
The session string can then be used in .env or added via admin API.
"""
import sys
import sqlite3
import struct
import base64
from pathlib import Path


def read_session_file(session_path: str) -> dict:
    """Read Pyrogram session file (SQLite database)."""
    path = Path(session_path)
    
    if not path.exists():
        raise FileNotFoundError(f"Session file not found: {session_path}")
    
    if not path.suffix == '.session':
        print(f"Warning: File doesn't have .session extension")
    
    conn = sqlite3.connect(session_path)
    cursor = conn.cursor()
    
    # Get session data
    cursor.execute("SELECT * FROM sessions")
    row = cursor.fetchone()
    
    if not row:
        raise ValueError("No session data found in file")
    
    # Pyrogram session table structure:
    # dc_id, api_id, test_mode, auth_key, date, user_id, is_bot
    columns = [desc[0] for desc in cursor.description]
    session_data = dict(zip(columns, row))
    
    conn.close()
    return session_data


def session_to_string(session_data: dict) -> str:
    """
    Convert session data to Pyrogram session string format.
    
    Session string format (base64):
    - 1 byte: dc_id
    - 4 bytes: api_id (little-endian)
    - 1 byte: test_mode
    - 256 bytes: auth_key
    - 8 bytes: user_id (little-endian)
    - 1 byte: is_bot
    """
    dc_id = session_data.get('dc_id', 1)
    api_id = session_data.get('api_id', 0)
    test_mode = session_data.get('test_mode', 0)
    auth_key = session_data.get('auth_key', b'')
    user_id = session_data.get('user_id', 0)
    is_bot = session_data.get('is_bot', 0)
    
    # Handle auth_key
    if isinstance(auth_key, str):
        auth_key = auth_key.encode()
    
    # Ensure auth_key is 256 bytes
    if len(auth_key) != 256:
        raise ValueError(f"Invalid auth_key length: {len(auth_key)} (expected 256)")
    
    # Pack the data
    packed = struct.pack(
        '<B I B 256s q B',  # little-endian: byte, int, byte, 256 bytes, long long, byte
        dc_id,
        api_id,
        test_mode,
        auth_key,
        user_id,
        is_bot
    )
    
    # Encode to base64
    session_string = base64.urlsafe_b64encode(packed).decode()
    
    return session_string


def main():
    if len(sys.argv) < 2:
        print("Usage: python convert_session.py <session_file>")
        print("\nExample:")
        print("  python convert_session.py my_account.session")
        sys.exit(1)
    
    session_path = sys.argv[1]
    
    try:
        print(f"Reading session file: {session_path}")
        session_data = read_session_file(session_path)
        
        print(f"\nSession info:")
        print(f"  DC ID: {session_data.get('dc_id')}")
        print(f"  API ID: {session_data.get('api_id')}")
        print(f"  User ID: {session_data.get('user_id')}")
        print(f"  Is Bot: {session_data.get('is_bot')}")
        
        session_string = session_to_string(session_data)
        
        print(f"\n{'='*60}")
        print("SESSION STRING:")
        print(f"{'='*60}")
        print(session_string)
        print(f"{'='*60}")
        
        print("\nYou can now:")
        print("1. Add to .env: SESSION_STRING=<string above>")
        print("2. Or add via API: POST /admin/userbots with session_string field")
        
    except FileNotFoundError as e:
        print(f"Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()


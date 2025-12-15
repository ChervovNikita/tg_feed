"""
Export Pyrogram session to string using Pyrogram library.

Usage:
    python scripts/export_session.py <session_name>
    
Where <session_name> is the name without .session extension.
Example: if you have "my_account.session", run:
    python scripts/export_session.py my_account
"""
import sys
import asyncio
from pathlib import Path

try:
    from pyrogram import Client
except ImportError:
    print("Pyrogram not installed. Run: pip install pyrogram")
    sys.exit(1)


async def export_session(session_name: str):
    """Export session file to session string."""
    
    # Check if session file exists
    session_file = Path(f"{session_name}.session")
    if not session_file.exists():
        print(f"Error: Session file '{session_file}' not found")
        print(f"Make sure the .session file is in the current directory")
        sys.exit(1)
    
    print(f"Found session file: {session_file}")
    
    # Create client with existing session
    # We need api_id and api_hash to create the client, 
    # but they don't matter for just exporting
    app = Client(
        name=session_name,
        api_id=1,  # placeholder, session already has this
        api_hash="placeholder"  # placeholder, session already has this
    )
    
    try:
        # Start the client (loads the session)
        await app.start()
        
        # Get user info
        me = await app.get_me()
        print(f"\nAccount info:")
        print(f"  User ID: {me.id}")
        print(f"  Username: @{me.username}" if me.username else "  Username: None")
        print(f"  Name: {me.first_name} {me.last_name or ''}")
        print(f"  Phone: {me.phone_number}")
        
        # Export session string
        session_string = await app.export_session_string()
        
        print(f"\n{'='*60}")
        print("SESSION STRING:")
        print(f"{'='*60}")
        print(session_string)
        print(f"{'='*60}")
        
        print("\nAdd this to your .env file:")
        print(f"SESSION_STRING={session_string}")
        
        await app.stop()
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


def main():
    if len(sys.argv) < 2:
        print("Usage: python export_session.py <session_name>")
        print("\nExample:")
        print("  python export_session.py my_account")
        print("  (for file my_account.session)")
        sys.exit(1)
    
    session_name = sys.argv[1]
    
    # Remove .session extension if provided
    if session_name.endswith('.session'):
        session_name = session_name[:-8]
    
    asyncio.run(export_session(session_name))


if __name__ == "__main__":
    main()


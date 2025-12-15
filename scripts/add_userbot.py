"""
Add userbot account to the pool.

Usage:
    python scripts/add_userbot.py

Will prompt for session_string, api_id, api_hash and encrypt them.
"""
import os
import sys
import asyncio
from pathlib import Path

# Add parent dir to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    import asyncpg
    from cryptography.fernet import Fernet
except ImportError as e:
    print(f"Missing dependency: {e}")
    print("Run: pip install asyncpg cryptography")
    sys.exit(1)

# Load .env
from dotenv import load_dotenv
load_dotenv()


def get_encryption_key() -> str:
    """Get or generate encryption key."""
    key = os.getenv('USERBOT_ENCRYPTION_KEY')
    
    if not key:
        print("\n⚠️  USERBOT_ENCRYPTION_KEY not found in .env")
        print("Generating new key...")
        key = Fernet.generate_key().decode()
        print(f"\nAdd this to your .env file:")
        print(f"USERBOT_ENCRYPTION_KEY={key}")
        print()
        
        # Ask to continue
        confirm = input("Continue with this key? (y/n): ")
        if confirm.lower() != 'y':
            sys.exit(0)
    
    return key


def encrypt(key: str, plaintext: str) -> str:
    """Encrypt a string with Fernet."""
    f = Fernet(key.encode())
    return f.encrypt(plaintext.encode()).decode()


async def add_account(
    database_url: str,
    encryption_key: str,
    phone_number: str,
    session_string: str,
    api_id: int,
    api_hash: str,
    max_channels: int = 50,
    join_cooldown_seconds: int = 60
):
    """Add userbot account to database."""
    
    # Encrypt sensitive data
    session_encrypted = encrypt(encryption_key, session_string)
    api_hash_encrypted = encrypt(encryption_key, api_hash)
    
    conn = await asyncpg.connect(database_url)
    
    try:
        # Check if table exists
        exists = await conn.fetchval("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'userbot_accounts'
            )
        """)
        
        if not exists:
            print("Creating userbot_accounts table...")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS userbot_accounts (
                    id SERIAL PRIMARY KEY,
                    phone_number TEXT UNIQUE,
                    session_encrypted TEXT NOT NULL,
                    api_id INT NOT NULL,
                    api_hash_encrypted TEXT NOT NULL,
                    status TEXT DEFAULT 'active',
                    last_join_at TIMESTAMP,
                    join_cooldown_seconds INT DEFAULT 60,
                    channels_count INT DEFAULT 0,
                    max_channels INT DEFAULT 50,
                    error_count INT DEFAULT 0,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS channel_userbot_assignments (
                    channel_id BIGINT NOT NULL,
                    userbot_id INT REFERENCES userbot_accounts(id) ON DELETE CASCADE,
                    assigned_at TIMESTAMP DEFAULT NOW(),
                    is_joined BOOLEAN DEFAULT FALSE,
                    join_attempts INT DEFAULT 0,
                    last_error TEXT,
                    PRIMARY KEY (channel_id)
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS channel_join_queue (
                    id SERIAL PRIMARY KEY,
                    channel_id BIGINT NOT NULL,
                    channel_username TEXT NOT NULL,
                    priority INT DEFAULT 0,
                    created_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(channel_id)
                )
            """)
        
        # Insert account
        row = await conn.fetchrow("""
            INSERT INTO userbot_accounts 
                (phone_number, session_encrypted, api_id, api_hash_encrypted, 
                 max_channels, join_cooldown_seconds)
            VALUES ($1, $2, $3, $4, $5, $6)
            RETURNING id, phone_number, status
        """, phone_number, session_encrypted, api_id, api_hash_encrypted,
             max_channels, join_cooldown_seconds)
        
        return row
        
    finally:
        await conn.close()


async def list_accounts(database_url: str):
    """List existing accounts."""
    conn = await asyncpg.connect(database_url)
    
    try:
        rows = await conn.fetch("""
            SELECT id, phone_number, status, channels_count, max_channels, 
                   error_count, created_at
            FROM userbot_accounts
            ORDER BY id
        """)
        return rows
    except:
        return []
    finally:
        await conn.close()


async def main():
    print("="*50)
    print("   Add Userbot Account to Pool")
    print("="*50)
    
    # Get database URL
    db_url = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/tg_filter')
    print(f"\nDatabase: {db_url.split('@')[1] if '@' in db_url else db_url}")
    
    # Get encryption key
    encryption_key = get_encryption_key()
    
    # Show existing accounts
    existing = await list_accounts(db_url)
    if existing:
        print(f"\nExisting accounts ({len(existing)}):")
        for acc in existing:
            print(f"  [{acc['id']}] {acc['phone_number']} - {acc['status']} ({acc['channels_count']}/{acc['max_channels']} channels)")
    
    print("\n" + "-"*50)
    print("Enter new account details:")
    print("-"*50)
    
    # Get input
    phone = input("Phone number (e.g. +79991234567): ").strip()
    session = input("Session string: ").strip()
    api_id = input("API ID: ").strip()
    api_hash = input("API Hash: ").strip()
    max_ch = input("Max channels [50]: ").strip() or "50"
    cooldown = input("Join cooldown seconds [60]: ").strip() or "60"
    
    if not all([session, api_id, api_hash]):
        print("Error: session_string, api_id and api_hash are required")
        sys.exit(1)
    
    try:
        api_id = int(api_id)
        max_ch = int(max_ch)
        cooldown = int(cooldown)
    except ValueError:
        print("Error: api_id, max_channels and cooldown must be numbers")
        sys.exit(1)
    
    # Confirm
    print(f"\nAdding account:")
    print(f"  Phone: {phone or 'not set'}")
    print(f"  API ID: {api_id}")
    print(f"  Max channels: {max_ch}")
    print(f"  Cooldown: {cooldown}s")
    
    confirm = input("\nConfirm? (y/n): ")
    if confirm.lower() != 'y':
        print("Cancelled")
        sys.exit(0)
    
    # Add account
    try:
        result = await add_account(
            database_url=db_url,
            encryption_key=encryption_key,
            phone_number=phone or None,
            session_string=session,
            api_id=api_id,
            api_hash=api_hash,
            max_channels=max_ch,
            join_cooldown_seconds=cooldown
        )
        
        print(f"\n✅ Account added successfully!")
        print(f"   ID: {result['id']}")
        print(f"   Phone: {result['phone_number']}")
        print(f"   Status: {result['status']}")
        
        print("\n🔄 Userbot will automatically pick up the new account within 60 seconds.")
        print("   (No restart needed!)")
        
    except asyncpg.UniqueViolationError:
        print("\n❌ Error: Account with this phone number already exists")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())


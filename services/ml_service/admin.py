"""Admin endpoints for managing userbot accounts."""
import logging
from typing import List, Optional
from datetime import datetime

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from cryptography.fernet import Fernet

from config import settings
from database import db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin", tags=["admin"])


# Pydantic models
class UserbotAccountCreate(BaseModel):
    """Request to create a new userbot account."""
    phone_number: Optional[str] = None
    session_string: str  # Will be encrypted
    api_id: int
    api_hash: str  # Will be encrypted
    max_channels: int = 50
    join_cooldown_seconds: int = 60


class UserbotAccountResponse(BaseModel):
    """Response with userbot account info (no secrets)."""
    id: int
    phone_number: Optional[str]
    status: str
    last_join_at: Optional[datetime]
    join_cooldown_seconds: int
    channels_count: int
    max_channels: int
    error_count: int
    created_at: datetime
    updated_at: datetime


class UserbotAccountList(BaseModel):
    """List of userbot accounts."""
    accounts: List[UserbotAccountResponse]
    total: int


class ChannelQueueItem(BaseModel):
    """Channel in join queue."""
    id: int
    channel_id: int
    channel_username: str
    priority: int
    created_at: datetime


class ChannelQueueList(BaseModel):
    """List of channels in queue."""
    items: List[ChannelQueueItem]
    total: int


# Encryption helper
def get_fernet() -> Fernet:
    """Get Fernet instance for encryption."""
    if not settings.userbot_encryption_key:
        raise HTTPException(
            status_code=500, 
            detail="USERBOT_ENCRYPTION_KEY not configured"
        )
    return Fernet(settings.userbot_encryption_key.encode())


def encrypt(plaintext: str) -> str:
    """Encrypt a string."""
    return get_fernet().encrypt(plaintext.encode()).decode()


def decrypt(ciphertext: str) -> str:
    """Decrypt a string."""
    return get_fernet().decrypt(ciphertext.encode()).decode()


@router.post("/userbots", response_model=UserbotAccountResponse)
async def create_userbot_account(account: UserbotAccountCreate):
    """
    Add a new userbot account to the pool.
    Session string and API hash will be encrypted.
    """
    try:
        # Encrypt sensitive data
        session_encrypted = encrypt(account.session_string)
        api_hash_encrypted = encrypt(account.api_hash)
        
        async with db.pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO userbot_accounts 
                    (phone_number, session_encrypted, api_id, api_hash_encrypted, 
                     max_channels, join_cooldown_seconds)
                VALUES ($1, $2, $3, $4, $5, $6)
                RETURNING id, phone_number, status, last_join_at, join_cooldown_seconds,
                          channels_count, max_channels, error_count, created_at, updated_at
            """, 
                account.phone_number, 
                session_encrypted, 
                account.api_id, 
                api_hash_encrypted,
                account.max_channels,
                account.join_cooldown_seconds
            )
            
            logger.info(f"Created userbot account {row['id']}")
            return UserbotAccountResponse(**dict(row))
            
    except Exception as e:
        logger.error(f"Error creating userbot account: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/userbots", response_model=UserbotAccountList)
async def list_userbot_accounts():
    """List all userbot accounts."""
    async with db.pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, phone_number, status, last_join_at, join_cooldown_seconds,
                   channels_count, max_channels, error_count, created_at, updated_at
            FROM userbot_accounts
            ORDER BY created_at DESC
        """)
        
        accounts = [UserbotAccountResponse(**dict(row)) for row in rows]
        return UserbotAccountList(accounts=accounts, total=len(accounts))


@router.get("/userbots/{account_id}", response_model=UserbotAccountResponse)
async def get_userbot_account(account_id: int):
    """Get userbot account details."""
    async with db.pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT id, phone_number, status, last_join_at, join_cooldown_seconds,
                   channels_count, max_channels, error_count, created_at, updated_at
            FROM userbot_accounts
            WHERE id = $1
        """, account_id)
        
        if not row:
            raise HTTPException(status_code=404, detail="Account not found")
        
        return UserbotAccountResponse(**dict(row))


@router.patch("/userbots/{account_id}/status")
async def update_userbot_status(account_id: int, status: str):
    """Update userbot account status (active, disabled, cooldown)."""
    if status not in ('active', 'disabled', 'cooldown'):
        raise HTTPException(status_code=400, detail="Invalid status")
    
    async with db.pool.acquire() as conn:
        result = await conn.execute("""
            UPDATE userbot_accounts SET status = $1, updated_at = NOW()
            WHERE id = $2
        """, status, account_id)
        
        if result == "UPDATE 0":
            raise HTTPException(status_code=404, detail="Account not found")
        
        return {"status": "ok", "new_status": status}


@router.delete("/userbots/{account_id}")
async def delete_userbot_account(account_id: int):
    """Delete a userbot account."""
    async with db.pool.acquire() as conn:
        result = await conn.execute("""
            DELETE FROM userbot_accounts WHERE id = $1
        """, account_id)
        
        if result == "DELETE 0":
            raise HTTPException(status_code=404, detail="Account not found")
        
        return {"status": "ok", "deleted_id": account_id}


@router.get("/channel-queue", response_model=ChannelQueueList)
async def get_channel_queue():
    """Get channels waiting to be joined."""
    async with db.pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, channel_id, channel_username, priority, created_at
            FROM channel_join_queue
            ORDER BY priority DESC, created_at ASC
        """)
        
        items = [ChannelQueueItem(**dict(row)) for row in rows]
        return ChannelQueueList(items=items, total=len(items))


@router.post("/channel-queue")
async def add_to_channel_queue(channel_id: int, channel_username: str, priority: int = 0):
    """Manually add a channel to the join queue."""
    async with db.pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO channel_join_queue (channel_id, channel_username, priority)
            VALUES ($1, $2, $3)
            ON CONFLICT (channel_id) DO UPDATE SET priority = $3
        """, channel_id, channel_username, priority)
        
        return {"status": "ok", "channel_username": channel_username}


@router.delete("/channel-queue/{queue_id}")
async def remove_from_channel_queue(queue_id: int):
    """Remove a channel from the join queue."""
    async with db.pool.acquire() as conn:
        result = await conn.execute("""
            DELETE FROM channel_join_queue WHERE id = $1
        """, queue_id)
        
        if result == "DELETE 0":
            raise HTTPException(status_code=404, detail="Queue item not found")
        
        return {"status": "ok", "deleted_id": queue_id}


@router.get("/assignments")
async def get_channel_assignments():
    """Get channel-to-userbot assignments."""
    async with db.pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT a.channel_id, a.userbot_id, a.is_joined, a.join_attempts, 
                   a.last_error, a.assigned_at, u.phone_number
            FROM channel_userbot_assignments a
            LEFT JOIN userbot_accounts u ON a.userbot_id = u.id
            ORDER BY a.assigned_at DESC
        """)
        
        return {
            "assignments": [dict(row) for row in rows],
            "total": len(rows)
        }


@router.get("/stats")
async def get_userbot_stats():
    """Get userbot pool statistics."""
    async with db.pool.acquire() as conn:
        stats = await conn.fetchrow("""
            SELECT 
                COUNT(*) FILTER (WHERE status = 'active') as active_accounts,
                COUNT(*) FILTER (WHERE status = 'banned') as banned_accounts,
                COUNT(*) FILTER (WHERE status = 'disabled') as disabled_accounts,
                SUM(channels_count) as total_channels,
                AVG(channels_count)::FLOAT as avg_channels_per_account
            FROM userbot_accounts
        """)
        
        queue_count = await conn.fetchval(
            "SELECT COUNT(*) FROM channel_join_queue"
        )
        
        return {
            "active_accounts": stats['active_accounts'] or 0,
            "banned_accounts": stats['banned_accounts'] or 0,
            "disabled_accounts": stats['disabled_accounts'] or 0,
            "total_channels_assigned": stats['total_channels'] or 0,
            "avg_channels_per_account": round(stats['avg_channels_per_account'] or 0, 1),
            "channels_in_queue": queue_count
        }


@router.post("/generate-encryption-key")
async def generate_encryption_key():
    """Generate a new Fernet encryption key."""
    key = Fernet.generate_key().decode()
    return {
        "key": key,
        "instruction": "Add this to your .env file as USERBOT_ENCRYPTION_KEY"
    }


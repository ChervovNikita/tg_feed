"""
Multi-account Userbot Pool Manager.

Manages a pool of Telegram userbot accounts to listen to channels
and forward posts to Kafka. Accounts and assignments are stored in DB.
"""
import asyncio
import json
import logging
import random
from datetime import datetime, timedelta
from typing import Optional, Dict, List
from dataclasses import dataclass

import asyncpg
from pyrogram import Client
from pyrogram.types import Message
from pyrogram.errors import (
    UsernameNotOccupied, UsernameInvalid, ChannelPrivate,
    UserAlreadyParticipant, InviteHashExpired, FloodWait,
    ChannelInvalid, AuthKeyUnregistered, SessionRevoked,
    UserDeactivated, UserDeactivatedBan
)
from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import settings
from crypto import decrypt_session

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class UserbotAccount:
    """Represents a userbot account from the pool."""
    id: int
    phone_number: str
    session_string: str  # Decrypted
    api_id: int
    api_hash: str  # Decrypted
    status: str
    last_join_at: Optional[datetime]
    join_cooldown_seconds: int
    channels_count: int
    max_channels: int
    client: Optional[Client] = None


class UserbotPoolManager:
    """Manages a pool of userbot accounts."""
    
    def __init__(self):
        self.db_pool: Optional[asyncpg.Pool] = None
        self.producer: Optional[KafkaProducer] = None
        self.accounts: Dict[int, UserbotAccount] = {}  # account_id -> account
        self.channel_to_account: Dict[int, int] = {}  # channel_id -> account_id
        self.last_message_ids: Dict[int, int] = {}  # channel_id -> last_msg_id
    
    async def start(self):
        """Start the userbot pool manager."""
        await self._connect_db()
        self._connect_kafka()
        
        # Load accounts from database
        await self._load_accounts()
        
        if not self.accounts:
            logger.warning("⚠️ No userbot accounts found. Add accounts via: python scripts/add_userbot.py")
            logger.info("Waiting for accounts to be added...")
        else:
            # Start all account clients
            await self._start_all_clients()
        
        # Load channel assignments
        await self._load_assignments()
        
        # Initialize last message IDs
        await self._init_last_message_ids()
        
        # Start background tasks
        asyncio.create_task(self._channel_join_worker())
        asyncio.create_task(self._health_check_worker())
        
        # Start polling loop
        logger.info(f"Starting polling with {len(self.accounts)} accounts...")
        while True:
            try:
                await self._poll_all_channels()
            except Exception as e:
                logger.error(f"Error in polling loop: {e}")
            await asyncio.sleep(settings.poll_interval_seconds)
    
    async def _connect_db(self):
        """Connect to database."""
        self.db_pool = await asyncpg.create_pool(
            settings.database_url,
            min_size=2,
            max_size=10
        )
        logger.info("Database connected")
    
    def _connect_kafka(self):
        """Connect Kafka producer."""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=settings.kafka_bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                acks='all',
                retries=3
            )
            logger.info("Kafka producer connected")
        except KafkaError as e:
            logger.error(f"Failed to connect Kafka: {e}")
            self.producer = None
    
    async def _load_accounts(self):
        """Load userbot accounts from database and decrypt credentials."""
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT id, phone_number, session_encrypted, api_id, api_hash_encrypted,
                       status, last_join_at, join_cooldown_seconds, channels_count, max_channels
                FROM userbot_accounts
                WHERE status IN ('active', 'cooldown')
            """)
            
            for row in rows:
                try:
                    session = decrypt_session(row['session_encrypted'])
                    api_hash = decrypt_session(row['api_hash_encrypted'])
                    
                    account = UserbotAccount(
                        id=row['id'],
                        phone_number=row['phone_number'] or f"account_{row['id']}",
                        session_string=session,
                        api_id=row['api_id'],
                        api_hash=api_hash,
                        status=row['status'],
                        last_join_at=row['last_join_at'],
                        join_cooldown_seconds=row['join_cooldown_seconds'],
                        channels_count=row['channels_count'],
                        max_channels=row['max_channels']
                    )
                    self.accounts[account.id] = account
                    logger.info(f"Loaded account {account.phone_number} (id={account.id})")
                    
                except Exception as e:
                    logger.error(f"Failed to decrypt account {row['id']}: {e}")
            
            logger.info(f"Loaded {len(self.accounts)} userbot accounts")
    
    async def _start_all_clients(self):
        """Start Pyrogram clients for all accounts."""
        for account_id, account in list(self.accounts.items()):
            try:
                client = Client(
                    name=f"userbot_{account_id}",
                    api_id=account.api_id,
                    api_hash=account.api_hash,
                    session_string=account.session_string,
                    in_memory=True
                )
                await client.start()
                account.client = client
                logger.info(f"Started client for account {account.phone_number}")
                
            except (AuthKeyUnregistered, SessionRevoked, UserDeactivated, UserDeactivatedBan) as e:
                logger.error(f"Account {account.phone_number} is banned/revoked: {e}")
                await self._mark_account_banned(account_id)
                del self.accounts[account_id]
                
            except Exception as e:
                logger.error(f"Failed to start client for {account.phone_number}: {e}")
                await self._mark_account_error(account_id, str(e))
    
    async def _mark_account_banned(self, account_id: int):
        """Mark account as banned and reassign its channels to other accounts."""
        async with self.db_pool.acquire() as conn:
            # Get channels assigned to this account
            channels = await conn.fetch("""
                SELECT a.channel_id, s.channel_username
                FROM channel_userbot_assignments a
                JOIN subscriptions s ON a.channel_id = s.channel_id
                WHERE a.userbot_id = $1 AND a.is_joined = TRUE
            """, account_id)
            
            # Mark account as banned
            await conn.execute("""
                UPDATE userbot_accounts SET status = 'banned', updated_at = NOW()
                WHERE id = $1
            """, account_id)
            
            # Reset assignments and add channels back to queue
            if channels:
                logger.warning(f"🔄 Reassigning {len(channels)} channels from banned account {account_id}")
                
                for ch in channels:
                    channel_id = ch['channel_id']
                    username = ch['channel_username']
                    
                    # Mark as not joined
                    await conn.execute("""
                        UPDATE channel_userbot_assignments 
                        SET is_joined = FALSE, userbot_id = NULL
                        WHERE channel_id = $1
                    """, channel_id)
                    
                    # Add back to join queue with high priority
                    if username:
                        await conn.execute("""
                            INSERT INTO channel_join_queue (channel_id, channel_username, priority)
                            VALUES ($1, $2, 10)
                            ON CONFLICT (channel_id) DO UPDATE SET priority = 10
                        """, channel_id, username)
                        
                logger.info(f"📋 Added {len(channels)} channels to join queue for reassignment")
    
    async def _mark_account_error(self, account_id: int, error: str):
        """Increment error count for account."""
        async with self.db_pool.acquire() as conn:
            await conn.execute("""
                UPDATE userbot_accounts 
                SET error_count = error_count + 1, updated_at = NOW()
                WHERE id = $1
            """, account_id)
    
    async def _load_assignments(self):
        """Load channel-to-account assignments."""
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT channel_id, userbot_id FROM channel_userbot_assignments
                WHERE is_joined = TRUE
            """)
            
            for row in rows:
                self.channel_to_account[row['channel_id']] = row['userbot_id']
            
            logger.info(f"Loaded {len(self.channel_to_account)} channel assignments")
    
    async def _init_last_message_ids(self):
        """Initialize last message IDs for all assigned channels."""
        for channel_id, account_id in self.channel_to_account.items():
            account = self.accounts.get(account_id)
            if not account or not account.client:
                continue
            
            try:
                async for msg in account.client.get_chat_history(channel_id, limit=1):
                    self.last_message_ids[channel_id] = msg.id
                    logger.debug(f"Channel {channel_id}: last_msg_id={msg.id}")
            except Exception as e:
                logger.warning(f"Could not get last message for {channel_id}: {e}")
                self.last_message_ids[channel_id] = 0
    
    async def _get_available_account_for_join(self) -> Optional[UserbotAccount]:
        """
        Get an account that can join a channel right now.
        Must have passed cooldown and have capacity.
        """
        now = datetime.utcnow()
        
        for account in self.accounts.values():
            # Skip banned or disabled accounts
            if account.status not in ('active', 'cooldown') or not account.client:
                continue
            
            if account.channels_count >= account.max_channels:
                continue
            
            # Check cooldown - last_join_at can be in the future for long cooldowns
            if account.last_join_at:
                if account.last_join_at > now:
                    # Long cooldown (FROZEN etc) - last_join_at is set in the future
                    logger.debug(f"Account {account.phone_number} on cooldown until {account.last_join_at}")
                    continue
                
                cooldown_end = account.last_join_at + timedelta(seconds=account.join_cooldown_seconds)
                if now < cooldown_end:
                    continue
                
                # Cooldown passed - restore to active if was in cooldown
                if account.status == 'cooldown':
                    account.status = 'active'
            
            return account
        
        return None
    
    async def _channel_join_worker(self):
        """Background worker that processes channel join queue."""
        while True:
            try:
                async with self.db_pool.acquire() as conn:
                    # Get next channel from queue
                    row = await conn.fetchrow("""
                        SELECT id, channel_id, channel_username 
                        FROM channel_join_queue
                        ORDER BY priority DESC, created_at ASC
                        LIMIT 1
                    """)
                    
                    if not row:
                        await asyncio.sleep(10)
                        continue
                    
                    queue_id = row['id']
                    channel_id = row['channel_id']
                    username = row['channel_username']
                    
                    # Get available account
                    account = await self._get_available_account_for_join()
                    
                    if not account:
                        logger.debug("No accounts available for join, waiting...")
                        await asyncio.sleep(30)
                        continue
                    
                    # Try to join
                    logger.info(f"Account {account.phone_number} joining @{username}...")
                    success, real_channel_id = await self._join_channel(account, username)
                    
                    if success:
                        # Use resolved channel_id
                        if real_channel_id:
                            channel_id = real_channel_id
                        
                        # Create assignment
                        await conn.execute("""
                            INSERT INTO channel_userbot_assignments (channel_id, userbot_id, is_joined)
                            VALUES ($1, $2, TRUE)
                            ON CONFLICT (channel_id) DO UPDATE SET 
                                userbot_id = $2, is_joined = TRUE, last_error = NULL
                        """, channel_id, account.id)
                        
                        # Update account stats
                        await conn.execute("""
                            UPDATE userbot_accounts 
                            SET channels_count = channels_count + 1, 
                                last_join_at = NOW(),
                                updated_at = NOW()
                            WHERE id = $1
                        """, account.id)
                        
                        # Update subscription with real channel_id
                        await conn.execute("""
                            UPDATE subscriptions SET channel_id = $1 WHERE channel_username = $2
                        """, channel_id, username)
                        
                        # Remove from queue
                        await conn.execute("DELETE FROM channel_join_queue WHERE id = $1", queue_id)
                        
                        # Update local state
                        self.channel_to_account[channel_id] = account.id
                        account.channels_count += 1
                        account.last_join_at = datetime.utcnow()
                        
                        logger.info(f"Successfully joined @{username} (channel_id={channel_id})")
                    
                    elif real_channel_id == -1:
                        # Channel doesn't exist - remove permanently
                        logger.warning(f"🗑️ Removing non-existent channel @{username} from queue and subscriptions")
                        await conn.execute("DELETE FROM channel_join_queue WHERE id = $1", queue_id)
                        await conn.execute("DELETE FROM subscriptions WHERE channel_username = $1", username)
                        await conn.execute("DELETE FROM channel_userbot_assignments WHERE channel_id = $1", channel_id)
                    
                    else:
                        # Update join attempts
                        await conn.execute("""
                            UPDATE channel_userbot_assignments 
                            SET join_attempts = join_attempts + 1, last_error = $2
                            WHERE channel_id = $1
                        """, channel_id, "Join failed")
                        
                        # Remove from queue after 3 attempts
                        attempts = await conn.fetchval("""
                            SELECT join_attempts FROM channel_userbot_assignments WHERE channel_id = $1
                        """, channel_id)
                        
                        if attempts and attempts >= 3:
                            await conn.execute("DELETE FROM channel_join_queue WHERE id = $1", queue_id)
                            logger.warning(f"Gave up on @{username} after 3 attempts")
                    
                    # Randomized delay between joins
                    delay = random.randint(settings.join_cooldown_seconds, settings.join_cooldown_seconds * 2)
                    await asyncio.sleep(delay)
                    
            except Exception as e:
                logger.error(f"Error in join worker: {e}")
                await asyncio.sleep(30)
    
    async def _join_channel(self, account: UserbotAccount, username: str) -> tuple[bool, Optional[int]]:
        """
        Try to join a channel.
        Returns (success, resolved_channel_id).
        """
        try:
            await account.client.join_chat(username)
            chat = await account.client.get_chat(username)
            return True, chat.id if chat else None
            
        except UserAlreadyParticipant:
            chat = await account.client.get_chat(username)
            return True, chat.id if chat else None
            
        except ChannelPrivate:
            logger.warning(f"Channel @{username} is private")
            return False, None
        
        except (UsernameNotOccupied, UsernameInvalid):
            logger.error(f"❌ Channel @{username} does not exist - removing from queue")
            # Mark for permanent removal
            return False, -1  # -1 signals "remove permanently"
            
        except FloodWait as e:
            logger.warning(f"FloodWait {e.value}s for account {account.phone_number}")
            # Put account on cooldown
            account.last_join_at = datetime.utcnow() + timedelta(seconds=e.value)
            return False, None
            
        except (UserDeactivated, UserDeactivatedBan) as e:
            logger.error(f"Account {account.phone_number} banned: {e}")
            await self._mark_account_banned(account.id)
            account.status = 'banned'
            return False, None
            
        except Exception as e:
            error_str = str(e)
            # Handle FROZEN_METHOD_INVALID - account is restricted from joining
            if 'FROZEN_METHOD_INVALID' in error_str:
                logger.error(f"🧊 Account {account.phone_number} is FROZEN - cannot join channels. Putting on 1 hour cooldown.")
                account.last_join_at = datetime.utcnow() + timedelta(hours=1)
                account.status = 'cooldown'
                # Update in DB
                async with self.db_pool.acquire() as conn:
                    await conn.execute("""
                        UPDATE userbot_accounts 
                        SET status = 'cooldown', last_join_at = NOW() + INTERVAL '1 hour'
                        WHERE id = $1
                    """, account.id)
                return False, None
            
            logger.error(f"Error joining @{username}: {e}")
            return False, None
    
    async def _health_check_worker(self):
        """Periodically check account health, sync new accounts, and reload subscriptions."""
        while True:
            try:
                await asyncio.sleep(60)  # Every minute
                
                # Sync accounts from database (add new, remove disabled)
                await self._sync_accounts()
                
                # Reload subscriptions and add new channels to queue
                await self._sync_channel_queue()
                
                # Check for stale accounts and clean up
                for account in list(self.accounts.values()):
                    if account.status == 'banned':
                        logger.info(f"🗑️ Cleaning up banned account {account.phone_number}")
                        
                        # Remove channel assignments from local cache
                        channels_to_remove = [
                            ch_id for ch_id, acc_id in self.channel_to_account.items() 
                            if acc_id == account.id
                        ]
                        for ch_id in channels_to_remove:
                            del self.channel_to_account[ch_id]
                            self.last_message_ids.pop(ch_id, None)
                        
                        if account.client:
                            try:
                                await account.client.stop()
                            except:
                                pass
                        del self.accounts[account.id]
                
                # Reload assignments to pick up reassigned channels
                await self._load_assignments()
                        
            except Exception as e:
                logger.error(f"Error in health check: {e}")
    
    async def _sync_accounts(self):
        """Sync accounts from database - add new ones, remove disabled."""
        async with self.db_pool.acquire() as conn:
            # Get all active/cooldown accounts from DB
            rows = await conn.fetch("""
                SELECT id, phone_number, session_encrypted, api_id, api_hash_encrypted,
                       status, last_join_at, join_cooldown_seconds, channels_count, max_channels
                FROM userbot_accounts
                WHERE status IN ('active', 'cooldown')
            """)
            
            db_account_ids = set()
            
            for row in rows:
                account_id = row['id']
                db_account_ids.add(account_id)
                
                # Skip if already loaded
                if account_id in self.accounts:
                    # Update state from DB
                    self.accounts[account_id].status = row['status']
                    self.accounts[account_id].channels_count = row['channels_count']
                    self.accounts[account_id].last_join_at = row['last_join_at']  # Sync cooldown
                    continue
                
                # New account - load and start
                try:
                    session = decrypt_session(row['session_encrypted'])
                    api_hash = decrypt_session(row['api_hash_encrypted'])
                    
                    account = UserbotAccount(
                        id=account_id,
                        phone_number=row['phone_number'] or f"account_{account_id}",
                        session_string=session,
                        api_id=row['api_id'],
                        api_hash=api_hash,
                        status=row['status'],
                        last_join_at=row['last_join_at'],
                        join_cooldown_seconds=row['join_cooldown_seconds'],
                        channels_count=row['channels_count'],
                        max_channels=row['max_channels']
                    )
                    
                    # Start client
                    client = Client(
                        name=f"userbot_{account_id}",
                        api_id=account.api_id,
                        api_hash=account.api_hash,
                        session_string=account.session_string,
                        in_memory=True
                    )
                    await client.start()
                    account.client = client
                    
                    self.accounts[account_id] = account
                    logger.info(f"🆕 Hot-loaded new account {account.phone_number} (id={account_id})")
                    
                except (AuthKeyUnregistered, SessionRevoked, UserDeactivated, UserDeactivatedBan) as e:
                    logger.error(f"New account {account_id} is banned/revoked: {e}")
                    await self._mark_account_banned(account_id)
                    
                except Exception as e:
                    logger.error(f"Failed to hot-load account {account_id}: {e}")
            
            # Remove accounts that are no longer active in DB
            for account_id in list(self.accounts.keys()):
                if account_id not in db_account_ids:
                    account = self.accounts[account_id]
                    logger.info(f"🗑️ Removing disabled account {account.phone_number}")
                    if account.client:
                        try:
                            await account.client.stop()
                        except:
                            pass
                    del self.accounts[account_id]
    
    async def _sync_channel_queue(self):
        """Sync subscriptions to channel join queue."""
        async with self.db_pool.acquire() as conn:
            # Find channels that need to be joined
            rows = await conn.fetch("""
                SELECT DISTINCT s.channel_id, s.channel_username
                FROM subscriptions s
                LEFT JOIN channel_userbot_assignments a ON s.channel_id = a.channel_id
                WHERE s.is_active = TRUE 
                  AND s.channel_username IS NOT NULL
                  AND (a.channel_id IS NULL OR a.is_joined = FALSE)
            """)
            
            for row in rows:
                if not row['channel_username']:
                    continue
                    
                # Add to queue if not already there
                await conn.execute("""
                    INSERT INTO channel_join_queue (channel_id, channel_username)
                    VALUES ($1, $2)
                    ON CONFLICT (channel_id) DO NOTHING
                """, row['channel_id'], row['channel_username'])
            
            if rows:
                logger.info(f"Added {len(rows)} channels to join queue")
    
    async def _poll_all_channels(self):
        """Poll all assigned channels for new messages."""
        for channel_id, account_id in list(self.channel_to_account.items()):
            account = self.accounts.get(account_id)
            if not account or not account.client or account.status != 'active':
                continue
            
            try:
                last_id = self.last_message_ids.get(channel_id, 0)
                if last_id == 0:
                    # Initialize
                    async for msg in account.client.get_chat_history(channel_id, limit=1):
                        self.last_message_ids[channel_id] = msg.id
                    continue
                
                new_messages = []
                async for msg in account.client.get_chat_history(channel_id, limit=5):
                    if msg.id <= last_id:
                        break
                    new_messages.append(msg)
                
                for msg in reversed(new_messages):
                    await self._process_message(msg, channel_id)
                    self.last_message_ids[channel_id] = max(
                        self.last_message_ids.get(channel_id, 0),
                        msg.id
                    )
                    
            except ChannelInvalid:
                logger.warning(f"Lost access to channel {channel_id}")
                del self.channel_to_account[channel_id]
                
            except FloodWait as e:
                logger.warning(f"FloodWait {e.value}s while polling")
                await asyncio.sleep(min(e.value, 60))
                
            except Exception as e:
                logger.error(f"Error polling channel {channel_id}: {e}")
    
    async def _process_message(self, message: Message, channel_id: int):
        """Process a channel message and send to Kafka."""
        try:
            media_urls = []
            if message.photo:
                media_urls.append(f"photo:{message.photo.file_id}")
            elif message.video:
                media_urls.append(f"video:{message.video.file_id}")
            elif message.document:
                if message.document.mime_type and message.document.mime_type.startswith('image'):
                    media_urls.append(f"document:{message.document.file_id}")
            
            text_preview = (message.text or message.caption or '[media]')[:50]
            logger.info(f"New post in channel {channel_id} (msg_id={message.id}): {text_preview}...")
            
            post_data = {
                'user_id': 0,
                'channel_id': channel_id,
                'message_id': message.id,
                'text': message.text or message.caption or '',
                'media_urls': media_urls,
                'timestamp': message.date.isoformat() if message.date else datetime.now().isoformat()
            }
            
            if self.producer:
                self.producer.send('raw_posts', value=post_data)
                self.producer.flush()
                logger.info(f"Sent post {message.id} to Kafka")
            else:
                logger.warning("Kafka producer not available")
                
        except Exception as e:
            logger.error(f"Error processing message: {e}")
    
    async def stop(self):
        """Stop all clients and connections."""
        for account in self.accounts.values():
            if account.client:
                try:
                    await account.client.stop()
                except:
                    pass
        
        if self.producer:
            self.producer.close()
        
        if self.db_pool:
            await self.db_pool.close()
        
        logger.info("Userbot pool manager stopped")


async def main():
    """Main entry point."""
    manager = UserbotPoolManager()
    
    try:
        await manager.start()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    finally:
        await manager.stop()


if __name__ == "__main__":
    asyncio.run(main())

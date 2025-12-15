"""
Medium Article Scraper.

Monitors Medium RSS feeds for new articles and sends them to Kafka
for processing by the ML service.

Tags are dynamically loaded from user subscriptions in the database.
"""
import asyncio
import json
import logging
import re
from datetime import datetime
from typing import Optional, Set, List

import asyncpg
import feedparser
import cloudscraper
from kafka import KafkaProducer
from kafka.errors import KafkaError

from config import settings

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MediumScraper:
    """Scrapes Medium articles and sends to Kafka."""
    
    def __init__(self):
        self.db_pool: Optional[asyncpg.Pool] = None
        self.producer: Optional[KafkaProducer] = None
        self.seen_urls: Set[str] = set()
        self.current_tags: Set[str] = set()
        self.scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "desktop": True}
        )
    
    def _parse_cookies(self, cookie_string: str) -> dict:
        """Parse cookie string into dict."""
        cookies = {}
        for item in cookie_string.split(';'):
            item = item.strip()
            if '=' in item:
                key, value = item.split('=', 1)
                cookies[key.strip()] = value.strip()
        return cookies
    
    async def start(self):
        """Start the scraper."""
        await self._connect_db()
        self._connect_kafka()
        
        # Load already processed articles
        await self._load_seen_articles()
        
        logger.info(f"Poll interval: {settings.poll_interval_seconds}s")
        logger.info("Tags will be loaded dynamically from user subscriptions")
        
        while True:
            try:
                # Refresh tags from database each iteration
                await self._refresh_tags()
                
                if self.current_tags:
                    await self._scrape_all_tags()
                else:
                    logger.info("No active tag subscriptions found, waiting...")
                    
            except Exception as e:
                logger.error(f"Error in scrape loop: {e}")
            
            await asyncio.sleep(settings.poll_interval_seconds)
    
    async def _connect_db(self):
        """Connect to database."""
        self.db_pool = await asyncpg.create_pool(
            settings.database_url,
            min_size=1,
            max_size=5
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
    
    async def _load_seen_articles(self):
        """Load already processed article URLs from database."""
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT source_url FROM posts WHERE source_url IS NOT NULL"
            )
            self.seen_urls = {row['source_url'] for row in rows}
            logger.info(f"Loaded {len(self.seen_urls)} already processed articles")
    
    async def _refresh_tags(self):
        """Load active tags from user subscriptions."""
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT tag FROM tag_subscriptions 
                WHERE is_active = TRUE
                """
            )
            new_tags = {row['tag'] for row in rows}
            
            # Fall back to config tags if no subscriptions
            if not new_tags:
                new_tags = set(settings.tags_list)
                logger.debug(f"No user subscriptions, using default tags: {new_tags}")
            
            # Log changes
            if new_tags != self.current_tags:
                added = new_tags - self.current_tags
                removed = self.current_tags - new_tags
                if added:
                    logger.info(f"📥 New tags to monitor: {added}")
                if removed:
                    logger.info(f"📤 Stopped monitoring tags: {removed}")
                    
            self.current_tags = new_tags
    
    async def _scrape_all_tags(self):
        """Scrape articles from all active tags."""
        logger.debug(f"Scraping {len(self.current_tags)} tags: {self.current_tags}")
        
        for tag in self.current_tags:
            try:
                await self._scrape_tag(tag)
            except Exception as e:
                logger.error(f"Error scraping tag '{tag}': {e}")
    
    async def _scrape_tag(self, tag: str):
        """Scrape articles from a single tag feed."""
        feed_url = f"https://medium.com/feed/tag/{tag}"
        
        try:
            # Use cloudscraper to bypass Cloudflare
            response = self.scraper.get(feed_url, timeout=30)
            if response.status_code != 200:
                logger.warning(f"Failed to fetch feed for tag '{tag}': HTTP {response.status_code}")
                return
            feed = feedparser.parse(response.text)
        except Exception as e:
            logger.error(f"Failed to parse feed for tag '{tag}': {e}")
            return
        
        new_articles = 0
        
        
        for entry in reversed(feed.entries):
            link = entry.get("link", "")
            # Clean URL
            link = link.split('?source=')[0]
            
            if not link or link in self.seen_urls:
                continue
            
            self.seen_urls.add(link)
            
            # Extract article ID
            article_id = self._extract_article_id(link)
            if not article_id:
                continue
            
            # Fetch full article text
            text = await self._fetch_article_text(article_id)
            if not text or len(text) < settings.min_article_length:
                logger.debug(f"Skipping article (too short or empty): {link}")
                continue
            
            # Extract metadata
            title = entry.get("title", "")
            author = self._extract_author(entry, link)
            published = entry.get("published", "")
            
            # Send to Kafka
            await self._send_to_kafka(
                article_id=article_id,
                url=link,
                title=title,
                text=text,
                author=author,
                tag=tag,
                published=published
            )
            
            new_articles += 1
            logger.info(f"📄 New article: {title[:50]}... by @{author} [#{tag}]")
            
            # Delay between articles to avoid rate limiting
            await asyncio.sleep(2)
        
        if new_articles > 0:
            logger.info(f"Found {new_articles} new articles in tag '{tag}'")
    
    def _extract_article_id(self, url: str) -> Optional[str]:
        """Extract Medium article ID from URL."""
        # Try format: -[12-char-hex]
        m = re.search(r"-([0-9a-fA-F]{12})(?:[/?#]|$)", url)
        if m:
            return m.group(1)
        
        # Try format without dash
        m = re.search(r"([0-9a-fA-F]{12})(?:[/?#]|$)", url)
        return m.group(1) if m else None
    
    def _extract_author(self, entry, link: str) -> Optional[str]:
        """Extract author username from feed entry."""
        # Try author_detail
        author_detail = getattr(entry, "author_detail", None)
        if author_detail:
            author_href = None
            if isinstance(author_detail, dict):
                author_href = author_detail.get("href")
            else:
                author_href = getattr(author_detail, "href", None)
            
            if author_href:
                m = re.search(r"/@([^/?#]+)", author_href)
                if m:
                    return m.group(1)
        
        # Try extracting from link
        m = re.search(r"medium\.com/@([^/?#]+)", link)
        if m:
            return m.group(1)
        
        return None
    
    async def _fetch_article_text(self, article_id: str) -> str:
        """Fetch full article text via GraphQL API."""
        if not settings.medium_cookie:
            logger.warning("MEDIUM_COOKIE not set - cannot fetch full article text")
            return ""
        
        url = "https://medium.com/_/graphql"
        
        # Parse cookies to dict to avoid encoding issues
        cookies = self._parse_cookies(settings.medium_cookie)
        
        headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "origin": "https://medium.com",
            "referer": "https://medium.com/",
            "user-agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/117.0.0.0 Safari/537.36"
            ),
        }
        
        # Extract XSRF token from cookies
        if 'xsrf' in cookies:
            headers["x-xsrf-token"] = cookies['xsrf']
        
        payload = {
            "operationName": "PostViewer",
            "variables": {"postId": article_id},
            "query": """
            query PostViewer($postId: ID!) {
              post(id: $postId) {
                id
                title
                content {
                  bodyModel {
                    paragraphs { text }
                  }
                }
                mediumUrl
              }
            }
            """
        }
        
        try:
            response = self.scraper.post(
                url, 
                headers=headers,
                cookies=cookies,
                data=json.dumps(payload), 
                timeout=30
            )
            
            # Check for Cloudflare block
            if response.status_code != 200:
                logger.warning(f"GraphQL HTTP {response.status_code} for article {article_id}")
                return ""
            
            raw = response.text.lstrip("])}while(1);</x>")
            
            # Check if we got HTML instead of JSON (Cloudflare challenge)
            if raw.strip().startswith("<!DOCTYPE") or raw.strip().startswith("<html"):
                logger.warning(f"GraphQL returned HTML (blocked) for article {article_id}")
                return ""
            
            if not raw.strip():
                logger.warning(f"GraphQL returned empty response for article {article_id}")
                return ""
                
            data = json.loads(raw)
            
            if "errors" in data:
                logger.warning(f"GraphQL error: {data['errors']}")
                return ""
            
            post = data.get("data", {}).get("post")
            if not post:
                return ""
            
            paragraphs = post.get("content", {}).get("bodyModel", {}).get("paragraphs", [])
            text = "\n".join(p.get("text", "") for p in paragraphs)
            return text.strip()
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error for article {article_id}: {e}")
            logger.debug(f"Response was: {response.text[:200]}")
            return ""
        except Exception as e:
            logger.error(f"Error fetching article {article_id}: {e}")
            return ""
    
    async def _send_to_kafka(
        self,
        article_id: str,
        url: str,
        title: str,
        text: str,
        author: Optional[str],
        tag: str,
        published: str
    ):
        """Send article to Kafka for processing."""
        if not self.producer:
            logger.warning("Kafka producer not available")
            return
        
        post_data = {
            'source': 'medium',
            'source_id': article_id,
            'source_url': url,
            'title': title,
            'text': text,
            'author': author,
            'tag': tag,
            'media_urls': [],  # Medium articles don't have media in this context
            'timestamp': published or datetime.utcnow().isoformat()
        }
        
        try:
            self.producer.send('raw_posts', value=post_data)
            self.producer.flush()
            logger.debug(f"Sent article to Kafka: {article_id}")
        except Exception as e:
            logger.error(f"Failed to send to Kafka: {e}")
    
    async def stop(self):
        """Stop the scraper."""
        if self.producer:
            self.producer.close()
        if self.db_pool:
            await self.db_pool.close()
        logger.info("Medium scraper stopped")


async def main():
    """Main entry point."""
    scraper = MediumScraper()
    
    try:
        await scraper.start()
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    finally:
        await scraper.stop()


if __name__ == "__main__":
    asyncio.run(main())

import configparser
import logging
import asyncio
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List
from telethon import TelegramClient, errors
from telethon.tl.types import Message
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import json
import signal
import pytz

class TelegramRealtyBot:
    def __init__(self, config_path: str = 'config.ini'):
        self.config_path = config_path
        self.config = None
        self.client = None
        self.scheduler = AsyncIOScheduler()
        self.daily_stats = {
            'posts_published': 0,
            'groups_targeted': 0,
            'errors': 0,
            'start_time': None
        }
        self.running = True
        self.setup_logging()
        self.load_config()

    def setup_logging(self):
        """Setup comprehensive logging, suppressing Telethon network noise"""
        log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        logging.basicConfig(
            level=logging.INFO,
            format=log_format,
            handlers=[
                logging.FileHandler(f'realty_bot_{datetime.now().strftime("%Y%m%d")}.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
        # Подавить ненужные логи Telethon
        logging.getLogger('telethon').setLevel(logging.WARNING)
        logging.getLogger('telethon.network').setLevel(logging.ERROR)
        logging.getLogger('telethon.client').setLevel(logging.ERROR)
        self.logger.info("Logging configured with suppressed Telethon network noise")

    def load_config(self):
        """Load and validate configuration"""
        try:
            self.config = configparser.ConfigParser()
            if not Path(self.config_path).exists():
                self.create_default_config()
                
            self.config.read(self.config_path)
            self.validate_config()
            self.logger.info("Configuration loaded successfully")
            
        except Exception as e:
            self.logger.error(f"Config loading failed: {e}")
            sys.exit(1)

    def create_default_config(self):
        """Create default configuration file"""
        config_content = """[Telegram]
api_id = YOUR_API_ID
api_hash = YOUR_API_HASH
source_channel = @domthailand
groups = @resnenko_v2,@resnenko_v1,@resnenko_v3,@resnenko_v4
repost_mode = forward
daily_reposts = 5
min_interval = 60
session_name = realty_bot_session

[Settings]
max_posts_per_day = 100
retry_attempts = 3
retry_delay = 5
enable_stats_report = true
stats_report_time = 23:59
timezone = Europe/Moscow

[Safety]
enable_flood_protection = true
max_posts_per_minute = 1
cooldown_on_error = 300
"""
        with open(self.config_path, 'w') as f:
            f.write(config_content)
        self.logger.info(f"Created default config at {self.config_path}")
        print(f"Please edit {self.config_path} with your API credentials and run again.")
        sys.exit(0)

    def validate_config(self):
        """Validate configuration parameters"""
        required_keys = {
            'Telegram': ['api_id', 'api_hash', 'source_channel', 'groups'],
            'Settings': [],
            'Safety': []
        }
        for section, keys in required_keys.items():
            if section not in self.config:
                raise ValueError(f"Missing section: {section}")
            for key in keys:
                if key not in self.config[section] or not self.config[section][key]:
                    raise ValueError(f"Missing required key: {section}.{key}")
        # Дополнительная валидация
        if int(self.config['Telegram']['daily_reposts']) < 1:
            raise ValueError("daily_reposts must be >= 1")

    async def initialize_client(self):
        """Initialize Telegram client"""
        try:
            api_id = int(self.config['Telegram']['api_id'])
            api_hash = self.config['Telegram']['api_hash']
            session_name = self.config.get('Telegram', 'session_name', fallback='realty_bot_session')
            
            self.client = TelegramClient(session_name, api_id, api_hash)
            await self.client.start()
            
            me = await self.client.get_me()
            
        except Exception as e:
            self.logger.error(f"Client initialization failed: {e}")
            raise

    async def get_today_posts(self) -> List[List[Message]]:
        """Fetch all posts from source channel published today, grouped by albums"""
        try:
            tz = pytz.timezone(self.config.get('Settings', 'timezone', fallback='Europe/Moscow'))
            #костыль ниже - он берет дату за 21.08.2025 и каждый раз тут придется переделывать , а также заново публиковать весь сет всех публикаций, при добавдении новых постов
            #потому что новые посты не войдут в этот сет публикаций за 21.08.2025 вот так было: today = datetime.now(tz).date()
            today = datetime(2025, 8, 21, tzinfo=tz).date()
            raw_messages = []
            source_channel = self.config['Telegram']['source_channel']
            max_posts = int(self.config.get('Settings', 'max_posts_per_day', fallback=10000))
            
            async for message in self.client.iter_messages(source_channel):
                if message.date.astimezone(tz).date() == today:
                    raw_messages.append(message)
                elif message.date.astimezone(tz).date() < today:
                    break
                if len(raw_messages) >= max_posts:
                    break
            
            posts = self._group_messages_by_album(raw_messages)
            
            self.logger.info(f"Found {len(posts)} posts ({sum(len(p) for p in posts)} messages) from today in {source_channel}")
            return posts
            
        except Exception as e:
            self.logger.error(f"Error fetching today's posts: {e}")
            return []

    def _group_messages_by_album(self, messages: List[Message]) -> List[List[Message]]:
        """Group messages by album (grouped_id) or treat single messages as individual posts"""
        grouped_posts = []
        grouped_messages = {}
        single_messages = []
        
        for message in messages:
            if message.grouped_id:
                if message.grouped_id not in grouped_messages:
                    grouped_messages[message.grouped_id] = []
                grouped_messages[message.grouped_id].append(message)
            else:
                single_messages.append([message])
        
        for grouped_id, group_msgs in grouped_messages.items():
            group_msgs.sort(key=lambda x: x.id)
            grouped_posts.append(group_msgs)
        
        grouped_posts.extend(single_messages)
        grouped_posts.sort(key=lambda group: group[0].id)
        
        return grouped_posts

    async def publish_post(self, group: str, post_group: List[Message], mode: str) -> bool:
        """Publish a single post (or album) to a group"""
        if not post_group:
            return False
            
        try:
            retry_attempts = int(self.config.get('Settings', 'retry_attempts', fallback=3))
            source_channel = self.config['Telegram']['source_channel']
            
            for attempt in range(retry_attempts):
                try:
                    if mode.lower() == 'forward':
                        message_ids = [msg.id for msg in post_group]
                        await self.client.forward_messages(group, message_ids, source_channel)
                        
                    elif mode.lower() == 'copy':
                        if len(post_group) == 1:
                            message = post_group[0]
                            if not message.message and not message.media:
                                return False
                            if message.media:
                                await self.client.send_file(
                                    group, 
                                    message.media, 
                                    caption=message.message or ""
                                )
                            else:
                                await self.client.send_message(group, message.message or "")
                        else:
                            media_files = []
                            caption = ""
                            for msg in post_group:
                                if msg.message and not caption:
                                    caption = msg.message
                                if msg.media:
                                    media_files.append(msg.media)
                            
                            if not media_files and not caption:
                                return False
                            if media_files:
                                await self.client.send_file(
                                    group,
                                    media_files,
                                    caption=caption
                                )
                            elif caption:
                                await self.client.send_message(group, caption)
                    
                    post_type = "album" if len(post_group) > 1 else "single"
                    first_id = post_group[0].id
                    self.logger.info(f"✓ Published {post_type} post {first_id} to {group} (mode: {mode}, {len(post_group)} msgs)")
                    self.daily_stats['posts_published'] += 1
                    return True
                    
                except errors.FloodWaitError as e:
                    wait_time = e.seconds
                    self.logger.warning(f"Flood wait for {wait_time}s in {group}")
                    if wait_time < 300:
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        raise
                        
                except errors.ChatWriteForbiddenError:
                    self.logger.error(f"No write permission in {group}")
                    return False
                    
                except Exception as e:
                    self.logger.error(f"Publish attempt {attempt+1} failed: {e}")
                    if attempt == retry_attempts - 1:
                        raise
                    retry_delay = int(self.config.get('Settings', 'retry_delay', fallback=5))
                    await asyncio.sleep(retry_delay)
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error publishing to {group}: {e}")
            self.daily_stats['errors'] += 1
            return False

    async def repost_cycle(self):
        """Execute one complete repost cycle"""
        try:
            today_posts = await self.get_today_posts()
            if not today_posts:
                self.logger.info("No posts found for today - skipping cycle")
                return

            groups = [g.strip() for g in self.config['Telegram']['groups'].split(',')]
            repost_mode = self.config['Telegram']['repost_mode']
            min_interval = int(self.config['Telegram']['min_interval'])
            
            self.daily_stats['groups_targeted'] = len(groups)
            
            total_messages = sum(len(post_group) for post_group in today_posts)
            self.logger.info(f"Starting repost cycle: {len(today_posts)} posts ({total_messages} messages) → {len(groups)} groups")
            
            for group in groups:
                self.logger.info(f"Processing group: {group}")
                
                for i, post_group in enumerate(today_posts):
                    if not self.running:
                        return
                        
                    success = await self.publish_post(group, post_group, repost_mode)
                    
                    if i < len(today_posts) - 1 and success:
                        await asyncio.sleep(min_interval)
                
                await asyncio.sleep(5)
            
            self.logger.info(f"Repost cycle completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error in repost cycle: {e}")
            self.daily_stats['errors'] += 1

    def schedule_jobs(self):
        """Schedule all repost jobs for the day"""
        try:
            tz = pytz.timezone(self.config.get('Settings', 'timezone', fallback='Europe/Moscow'))
            daily_reposts = int(self.config['Telegram']['daily_reposts'])
            interval_hours = 24 / daily_reposts
            
            self.scheduler.timezone = tz
            now = datetime.now(tz)
            for i in range(daily_reposts):
                run_time = now + timedelta(hours=i * interval_hours)
                self.scheduler.add_job(
                    self.repost_cycle,
                    'date',
                    run_date=run_time,
                    id=f'repost_{i}',
                    max_instances=1
                )
                self.logger.info(f"Scheduled repost {i+1}/{daily_reposts} at {run_time.strftime('%H:%M')}")
            
            if self.config.getboolean('Settings', 'enable_stats_report', fallback=True):
                stats_time = self.config.get('Settings', 'stats_report_time', fallback='23:59')
                hour, minute = map(int, stats_time.split(':'))
                
                stats_datetime = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
                if stats_datetime <= now:
                    stats_datetime += timedelta(days=1)
                
                self.scheduler.add_job(
                    self.print_daily_stats,
                    'date',
                    run_date=stats_datetime,
                    id='daily_stats'
                )
            
            self.scheduler.start()
            self.logger.info(f"Scheduler started with {daily_reposts} daily cycles")
            
        except Exception as e:
            self.logger.error(f"Error scheduling jobs: {e}")
            raise

    async def print_daily_stats(self):
        """Print comprehensive daily statistics"""
        tz = pytz.timezone(self.config.get('Settings', 'timezone', fallback='Europe/Moscow'))
        runtime = datetime.now(tz) - self.daily_stats['start_time']
        success_rate = (self.daily_stats['posts_published'] / 
                       max(1, self.daily_stats['posts_published'] + self.daily_stats['errors'])) * 100
        
        stats_report = f"""
📊 DAILY STATISTICS REPORT
{'='*40}
📅 Date: {datetime.now(tz).strftime('%Y-%m-%d')}
⏱️  Runtime: {str(runtime).split('.')[0]}
📝 Posts Published: {self.daily_stats['posts_published']}
🎯 Groups Targeted: {self.daily_stats['groups_targeted']}
❌ Errors: {self.daily_stats['errors']}
📈 Success Rate: {success_rate:.1f}%
{'='*40}
        """
        
        self.logger.info(stats_report)
        
        stats_file = f"daily_stats_{datetime.now(tz).strftime('%Y%m%d')}.json"
        with open(stats_file, 'w') as f:
            json.dump({
                'date': datetime.now(tz).isoformat(),
                'runtime_seconds': int(runtime.total_seconds()),
                **self.daily_stats
            }, f, indent=2)
        
        # Reset stats for next day
        self.daily_stats['posts_published'] = 0
        self.daily_stats['errors'] = 0
        self.daily_stats['groups_targeted'] = 0

    def setup_signal_handlers(self):
        """Setup graceful shutdown handlers"""
        def signal_handler(signum, frame):
            self.logger.info(f"Received signal {signum}, shutting down gracefully...")
            self.running = False
            if self.scheduler.running:
                self.scheduler.shutdown()
            asyncio.create_task(self.shutdown())
        
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    async def shutdown(self):
        """Graceful shutdown"""
        self.logger.info("Shutting down bot...")
        await self.print_daily_stats()
        if self.client and self.client.is_connected():
            await self.client.disconnect()
        self.logger.info("Bot shutdown complete")

    async def run(self):
        """Main run method"""
        try:
            tz = pytz.timezone(self.config.get('Settings', 'timezone', fallback='Europe/Moscow'))
            self.daily_stats['start_time'] = datetime.now(tz)
            self.setup_signal_handlers()
            
            await self.initialize_client()
            self.schedule_jobs()
            
            self.logger.info("🚀 Telegram Realty Bot started successfully!")
            self.logger.info("Press Ctrl+C to stop the bot")
            
            while self.running:
                await asyncio.sleep(1)
                
        except KeyboardInterrupt:
            self.logger.info("Bot stopped by user")
        except Exception as e:
            self.logger.error(f"Bot crashed: {e}", exc_info=True)
        finally:
            await self.shutdown()

def main():
    """Entry point with proper error handling"""
    try:
        bot = TelegramRealtyBot()
        asyncio.run(bot.run())
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
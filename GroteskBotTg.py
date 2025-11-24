import json, time, asyncio, logging, colorama, subprocess, shutil, traceback, urllib.parse, re, html, io, uuid, requests, sqlite3
from telegram.constants import ParseMode
from collections import defaultdict, namedtuple
from datetime import datetime
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from telegram import Bot
from telegram.error import RetryAfter, TimedOut
from config import TELEGRAM_BOT_TOKEN, EXCHANGERATE_API_KEY, BASE_URLS
from colorama import Fore, Back, Style
from PIL import Image, ImageDraw, ImageFont
from asyncio import Semaphore
import aiosqlite
from olx_scraper import run_olx_scraper

# Initialize constants and globals
colorama.init(autoreset=True)
BOT_VERSION, DB_NAME = "4.1.0", "shoes.db"
LIVE_MODE, ASK_FOR_LIVE_MODE = False, False
PAGE_SCRAPE = True
SHOE_DATA_FILE, EXCHANGE_RATES_FILE = 'shoe_data.json', 'exchange_rates.json'
COUNTRIES = ['IT', 'PL', 'US', 'GB']
BLOCK_RESOURCES = False

# Config-driven priorities and thresholds with safe defaults (compact, safe getattr)
COUNTRY_PRIORITY = ["PL", "US", "IT", "GB"]
SALE_EMOJI_ROCKET_THRESHOLD, SALE_EMOJI_UAH_THRESHOLD = 75, 2600
try:
    import config as _conf
    COUNTRY_PRIORITY = getattr(_conf, 'COUNTRY_PRIORITY', COUNTRY_PRIORITY)
    SALE_EMOJI_ROCKET_THRESHOLD = getattr(_conf, 'SALE_EMOJI_ROCKET_THRESHOLD', SALE_EMOJI_ROCKET_THRESHOLD)
    SALE_EMOJI_UAH_THRESHOLD = getattr(_conf, 'SALE_EMOJI_UAH_THRESHOLD', SALE_EMOJI_UAH_THRESHOLD)
    BLOCK_RESOURCES = getattr(_conf, 'BLOCK_RESOURCES', BLOCK_RESOURCES)
except Exception:
    pass

# Database semaphore to prevent concurrent access issues
DB_SEMAPHORE = Semaphore(1)

# Define namedtuples and container classes
ConversionResult = namedtuple('ConversionResult', ['uah_amount', 'exchange_rate', 'currency_symbol'])

# Statistics tracking
max_wait_times = {'url_changes': 0, 'final_url_changes': 0}
link_statistics = {
    'lyst_track_lead': {'success': 0, 'fail': 0, 'fail_links': []}, 'click_here': {'success': 0, 'fail': 0, 'fail_links': []},
    'other_failures': {'count': 0, 'links': []}, 'steps': {
        'Initial URL change': {'count': 0, 'final_url_obtained': 0}, 'After some waiting': {'count': 0, 'final_url_obtained': 0},
        'After Click here': {'count': 0, 'final_url_obtained': 0}, 'Track Lead': {'count': 0, 'final_url_obtained': 0}, 'Unknown': {'count': 0, 'final_url_obtained': 0}
    }
}

class ColoredFormatter(logging.Formatter):
    COLORS = {'DEBUG': Fore.CYAN, 'INFO': Fore.WHITE, 'WARNING': Fore.YELLOW, 'ERROR': Fore.RED, 
              'CRITICAL': Fore.RED + Back.WHITE, 'STAT': Fore.MAGENTA, 'GOOD': Fore.GREEN, 
              'LIGHTBLUE_INFO': Fore.LIGHTBLUE_EX}

    def format(self, record):
        log_color = self.COLORS.get(record.levelname, Fore.WHITE)
        timestamp = Fore.LIGHTBLACK_EX + self.formatTime(record, self.datefmt) + Style.RESET_ALL
        return f"{timestamp}     {log_color}{record.getMessage()}{Style.RESET_ALL}"

class TelegramMessageQueue:
    def __init__(self, bot_token):
        self.queue, self.bot_token, self.pending_messages = asyncio.Queue(), bot_token, {}

    async def add_message(self, chat_id, message, image_url=None, uah_price=None, sale_percentage=None):
        message_id = str(uuid.uuid4())
        self.pending_messages[message_id] = False
        await self.queue.put((message_id, chat_id, message, image_url, uah_price, sale_percentage))
        return message_id

    async def process_queue(self):
        while True:
            message_id, chat_id, message, image_url, uah_price, sale_percentage = await self.queue.get()
            success = await send_telegram_message(self.bot_token, chat_id, message, image_url, uah_price, sale_percentage)
            self.pending_messages[message_id] = success
            if not success:
                await self.queue.put((message_id, chat_id, message, image_url, uah_price, sale_percentage))
            await asyncio.sleep(1)

    def is_message_sent(self, message_id):
        return self.pending_messages.get(message_id, False)

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
if logger.hasHandlers():
    logger.handlers.clear()
handler = logging.StreamHandler()
handler.setFormatter(ColoredFormatter('%(asctime)s', datefmt='%d.%m %H:%M:%S'))
logger.addHandler(handler)

class SpecialLogger:
    @staticmethod
    def stat(message): logger.log(35, message)
    @staticmethod
    def good(message): logger.log(25, message)
    @staticmethod
    def info(message): logger.log(22, message)

special_logger = SpecialLogger()

# Add custom log levels
for level_name, level_num in [("STAT", 35), ("GOOD", 25), ("LIGHTBLUE_INFO", 22)]:
    logging.addLevelName(level_num, level_name)

class BrowserPool:
    def __init__(self, max_browsers=6):
        self.max_browsers, self._semaphore = max_browsers, Semaphore(max_browsers)
        self._playwright, self._browser_type = None, None

    async def init(self):
        if not self._playwright:
            self._playwright = await async_playwright().start()
            self._browser_type = self._playwright.firefox

    async def close(self):
        if self._playwright:
            await self._playwright.stop()
            self._playwright = None

    async def get_browser(self):
        await self.init()
        await self._semaphore.acquire()
        browser = await self._browser_type.launch(headless=not LIVE_MODE)
        return BrowserWrapper(browser, self._semaphore)

class BrowserWrapper:
    def __init__(self, browser, semaphore):
        self.browser, self._semaphore = browser, semaphore

    async def __aenter__(self):
        return self.browser

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.browser.close()
        self._semaphore.release()

browser_pool = BrowserPool(max_browsers=6)

# Helper functions
def clean_link_for_display(link):
    cleaned_link = re.sub(r'^(https?://)?(www\.)?', '', link)
    return (cleaned_link[:22] + '...') if len(cleaned_link) > 25 else cleaned_link

def load_font(font_size):
    for font_file in ["SFPro-Bold.ttf", "arialbd.ttf"]:
        try: return ImageFont.truetype(font_file, font_size)
        except IOError: continue
    return ImageFont.load_default()

def process_image(image_url, uah_price, sale_percentage):
    response = requests.get(image_url)
    try:
        img = Image.open(io.BytesIO(response.content))
        width, height = [dim * 2 for dim in img.size]
        img = img.resize((width, height), Image.LANCZOS)

        font_size = int(min(width, height) * 0.055)
        font = load_font(font_size)
        draw = ImageDraw.Draw(img)

        price_text, sale_text = f"{uah_price} UAH", f"-{sale_percentage}%"
        price_bbox = draw.textbbox((0, 0), price_text, font=font)
        sale_bbox = draw.textbbox((0, 0), sale_text, font=font)
        text_height = max(price_bbox[3] - price_bbox[1], sale_bbox[3] - sale_bbox[1])

        padding = 10
        bottom_area = text_height + (padding * 2)
        new_img = Image.new('RGBA', (width, height + bottom_area), (0, 0, 0, 0))
        new_img.paste(img, (0, 0))
        new_img.paste(Image.new('RGBA', (width, bottom_area), (0, 0, 0, 0)), (0, height))

        draw = ImageDraw.Draw(new_img)
        text_y = height + padding + (text_height // 2)
        draw.text((60, text_y), price_text, font=font, fill=(22,22,24), anchor="lm")
        draw.text((width - 60, text_y), sale_text, font=font, fill=(255,59,48), anchor="rm")

        img_byte_arr = io.BytesIO()
        new_img.save(img_byte_arr, format='PNG', quality=95)
        img_byte_arr.seek(0)
        return img_byte_arr
    finally:
        response.close()

# Database functions
PRAGMA_STATEMENTS = ['PRAGMA foreign_keys = ON','PRAGMA journal_mode = WAL','PRAGMA synchronous = NORMAL','PRAGMA busy_timeout = 30000']

def connect_db():
    conn = sqlite3.connect(DB_NAME, timeout=30.0)
    for stmt in PRAGMA_STATEMENTS:
        conn.execute(stmt)
    return conn

def create_tables():
    conn = connect_db()
    conn.executescript('''
    CREATE TABLE IF NOT EXISTS shoes (
        key TEXT PRIMARY KEY, name TEXT, unique_id TEXT,
        original_price TEXT, sale_price TEXT, image_url TEXT,
        store TEXT, country TEXT, shoe_link TEXT,
        lowest_price TEXT, lowest_price_uah REAL,
        uah_price REAL, active INTEGER);
    CREATE TABLE IF NOT EXISTS processed_shoes (
        key TEXT PRIMARY KEY, active INTEGER DEFAULT 1);
    CREATE INDEX IF NOT EXISTS idx_processed_shoes_active 
        ON processed_shoes(key) WHERE active = 1;
    CREATE INDEX IF NOT EXISTS idx_shoe_active ON shoes (active, country, uah_price);
    ''')
    conn.commit(); conn.close()

async def db_operation_with_retry(operation_func, max_retries=3):
    """Helper function to handle database operations with retry logic"""
    async with DB_SEMAPHORE:
        for attempt in range(max_retries):
            try:
                async with aiosqlite.connect(DB_NAME, timeout=30.0) as conn:
                    for stmt in PRAGMA_STATEMENTS:
                        await conn.execute(stmt)
                    return await operation_func(conn)
            except Exception as e:
                if "database is locked" in str(e).lower() and attempt < max_retries - 1:
                    logger.warning(f"Database locked, retrying in {2 ** attempt} seconds (attempt {attempt + 1}/{max_retries})")
                    await asyncio.sleep(2 ** attempt)
                    continue
                raise

async def is_shoe_processed(key):
    async def _operation(conn):
        async with conn.execute("SELECT 1 FROM processed_shoes WHERE key = ?", (key,)) as cursor:
            return await cursor.fetchone() is not None
    return await db_operation_with_retry(_operation)

async def mark_shoe_processed(key):
    async def _operation(conn):
        await conn.execute("INSERT OR IGNORE INTO processed_shoes(key, active) VALUES (?, 1)", (key,))
        await conn.commit()
    await db_operation_with_retry(_operation)

def load_shoe_data_from_db():
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM shoes')
    data = {row[0]: {
        'name': row[1], 'unique_id': row[2], 'original_price': row[3],
        'sale_price': row[4], 'image_url': row[5], 'store': row[6],
        'country': row[7], 'shoe_link': row[8], 'lowest_price': row[9],
        'lowest_price_uah': row[10], 'uah_price': row[11], 'active': bool(row[12])
    } for row in cursor.fetchall()}
    conn.close()
    return data

def load_shoe_data_from_json():
    try:
        with open(SHOE_DATA_FILE, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

async def save_shoe_data_bulk(shoes):
    """Save multiple shoes to database in a single transaction."""
    async def _operation(conn):
        data = [(
            s['key'], s['name'], s['unique_id'], s['original_price'], s['sale_price'], s['image_url'],
            s['store'], s['country'], s.get('shoe_link', ''), s.get('lowest_price', ''), s.get('lowest_price_uah', 0.0),
            s.get('uah_price', 0.0), 1 if s.get('active', True) else 0
        ) for s in shoes]
        await conn.executemany('''INSERT OR REPLACE INTO shoes (
            key, name, unique_id, original_price, sale_price,
            image_url, store, country, shoe_link, lowest_price,
            lowest_price_uah, uah_price, active
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)''', data)
        await conn.commit()
    
    await db_operation_with_retry(_operation)

async def async_save_shoe_data(shoe_data):
    shoes = [dict(shoe, key=key) for key, shoe in shoe_data.items()]
    await save_shoe_data_bulk(shoes)

async def migrate_json_to_sqlite():
    async def _operation(conn):
        async with conn.execute('SELECT COUNT(*) FROM shoes') as cursor:
            return (await cursor.fetchone())[0]
    if await db_operation_with_retry(_operation) == 0:
        data = load_shoe_data_from_json()
        if data: await async_save_shoe_data(data)

async def load_shoe_data():
    create_tables()
    await migrate_json_to_sqlite()
    return load_shoe_data_from_db()

async def save_shoe_data(data):
    await async_save_shoe_data(data)

# Web scraping and browser functions
async def handle_route(route):
    if route.request.resource_type in ["image", "media", "font", "stylesheet"]:
        await route.abort()
    else:
        await route.continue_()

async def scroll_page(page, max_attempts=None):
    SCROLL_PAUSE_TIME = 1
    SCROLL_STEP = 5000 if BLOCK_RESOURCES else 800
    if max_attempts is None:
        max_attempts = 10 if PAGE_SCRAPE else 300
    last_height = await page.evaluate("document.body.scrollHeight")
    total_scrolled, scroll_attempts = 0, 0

    while scroll_attempts < max_attempts:
        await page.evaluate(f"window.scrollBy(0, {SCROLL_STEP})")
        total_scrolled += SCROLL_STEP
        await asyncio.sleep(SCROLL_PAUSE_TIME)
        new_height = await page.evaluate("document.body.scrollHeight")
        
        if total_scrolled > new_height: break
        # scroll_attempts = 0 if new_height > last_height else scroll_attempts + 1
        scroll_attempts += 1

        last_height = new_height

async def get_page_content(url, country, max_scroll_attempts=None):
    async with (await browser_pool.get_browser()) as browser:
        context = await browser.new_context()
        await context.add_cookies([{'name': 'country', 'value': country, 'domain': '.lyst.com', 'path': '/'}])
        page = await context.new_page()
        if BLOCK_RESOURCES:
            await page.route("**/*", handle_route)
        await page.goto(url)
        await scroll_page(page, max_scroll_attempts)
        try:
            await page.wait_for_selector('._693owt3', timeout=10000)
            return await page.content()
        except: return None
        finally: await context.close()

async def get_soup(url, country, max_retries=3, max_scroll_attempts=None):
    for attempt in range(max_retries):
        try:
            content = await get_page_content(url, country, max_scroll_attempts)
            if not content:
                return None
            try:
                return BeautifulSoup(content, 'lxml')
            except Exception:
                return BeautifulSoup(content, 'html.parser')
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Failed to get soup (attempt {attempt + 1}/{max_retries}). Retrying...")
                await asyncio.sleep(5)
            else:
                logger.error(f"Failed to get soup for {url}")
                raise

def is_lyst_domain(url):
    return 'lyst.com' in urllib.parse.urlparse(url).netloc

def extract_embedded_url(url):
    parsed = urllib.parse.urlparse(url); qs = urllib.parse.parse_qs(parsed.query)
    for p in ('URL','murl','destination','url'):
        v = qs.get(p)
        if v: return urllib.parse.unquote(v[0])
    return url

async def get_final_clear_link(initial_url, semaphore, item_name, country, current_item, total_items):
    logger.info(f"Processing final link for {item_name} | Country: {country} | Progress: {current_item}/{total_items}")
    async with (await browser_pool.get_browser()) as browser:
        context = await browser.new_context()
        page = await context.new_page()
        steps_info = {'steps_taken': [], 'final_step': None, 'initial_url': initial_url, 'final_url': None}
        
        try:
            if BLOCK_RESOURCES:
                await page.route("**/*", handle_route)
            await page.goto(initial_url)
            # Step 1: Initial URL change
            start_time = time.time()
            await page.wait_for_url(lambda url: url != initial_url, timeout=20000)
            wait_time = time.time() - start_time
            max_wait_times['url_changes'] = max(max_wait_times['url_changes'], wait_time)
            current_step = 'Initial URL change'
            steps_info['steps_taken'].append(current_step)
            link_statistics['steps'][current_step]['count'] += 1

            await asyncio.sleep(5)
            current_url = extract_embedded_url(page.url)

            if not is_lyst_domain(current_url):
                steps_info['final_step'] = current_step
                steps_info['final_url'] = current_url
                link_statistics['steps'][current_step]['final_url_obtained'] += 1
            elif "lyst.com" in current_url and "return" in current_url:
                await page.goto(current_url)
                await page.wait_for_load_state('networkidle')
                current_step = 'After some waiting'
                
                if not is_lyst_domain(current_url):
                    steps_info['final_step'] = current_step
                    steps_info['final_url'] = current_url
                    link_statistics['steps'][current_step]['final_url_obtained'] += 1
            
            # Set default if not already set            
            if steps_info['final_url'] is None:
                steps_info['final_url'] = current_url
                steps_info['final_step'] = 'Unknown'
                current_step = 'Unknown'
                link_statistics['steps'][current_step]['count'] += 1

            final_url = urllib.parse.unquote(steps_info['final_url'])
            logger.info(f"Final link obtained for: {item_name}")
            return final_url
        except Exception:
            link_statistics['other_failures']['count'] += 1
            link_statistics['other_failures']['links'].append(initial_url)
            return initial_url
        finally:
            await context.close()

# Data extraction and processing
def extract_price(price_str):
    price_num = re.sub(r'[^\d.]', '', price_str)
    try: return float(price_num)
    except ValueError: return 0

def extract_shoe_data(card, country):
    if not card:
        logger.warning("Received None card in extract_shoe_data")
        return None
        
    try:
        # Extract name via a few fallback strategies
        finders = [
            lambda: card.find_all('span', class_=lambda x: x and 'vjlibs5' in x),
            lambda: card.find_all('span', class_=lambda x: x and 'vjlibs5' in x and 'vjlibs2' in x),
            lambda: card.find_all('span', class_=re.compile(r'.*vjlibs5.*')),
            lambda: card.find_all('span', class_=lambda x: x and ('_1b08vvh31' in x and 'vjlibs' in x)),
        ]
        name_elements = []
        for fn in finders:
            name_elements = fn()
            if name_elements: break
        if not name_elements:
            logger.warning(f"No name elements found. Card HTML structure:")
            debug_spans = card.find_all('span', class_=re.compile(r'.*vjlibs.*'))
            for i, span in enumerate(debug_spans[:5]):
                logger.warning(f"  Debug span {i}: class='{span.get('class')}', text='{span.text.strip()[:50]}'")
            return None
        full_name = ' '.join(e.text.strip() for e in name_elements if e and e.text)
        if 'Giuseppe Zanotti' in full_name: return None
        
        # Extract price elements with strategy fallbacks
        price_div = card.find('div', class_='ducdwf0')
        if not price_div:
            logger.warning("Price div not found")
            return None
        strategies = [
            lambda: (
                price_div.find('div', class_=lambda x: x and '_1b08vvhr6' in x and 'vjlibs1' in x),
                price_div.find('div', class_=lambda x: x and '_1b08vvh36' in x and 'vjlibs2' in x)
            ),
            lambda: (
                price_div.find('div', class_=lambda x: x and ('_1b08vvhos' in x and 'vjlibs1' in x)),
                price_div.find('div', class_=lambda x: x and ('_1b08vvh1w' in x and 'vjlibs2' in x))
            ),
            lambda: (
                price_div.find('div', class_=lambda x: x and 'vjlibs1' in x and 'vjlibs2' in x and '_1b08vvhq2' in x and '_1b08vvh36' not in x),
                price_div.find('div', class_=lambda x: x and 'vjlibs2' in x and '_1b08vvh36' in x)
            ),
            lambda: (
                price_div.find('div', class_=lambda x: x and 'vjlibs1' in x and '_1b08vvhnk' in x and '_1b08vvh1q' not in x),
                price_div.find('div', class_=lambda x: x and 'vjlibs2' in x and '_1b08vvh1q' in x) or
                price_div.find('div', class_=lambda x: x and '_1b08vvh1w' in x)
            ),
        ]
        original_price_elem = sale_price_elem = None
        for strat in strategies:
            o, s = strat()
            if o and s and o != s:
                original_price_elem, sale_price_elem = o, s
                break
        if not original_price_elem or not sale_price_elem:
            logger.warning("Price elements not found")
            return None
        original_price = original_price_elem.text.strip() if original_price_elem.text else "N/A"
        sale_price = sale_price_elem.text.strip() if sale_price_elem.text else "N/A"
        if extract_price(original_price) < 80:
            logger.info(f"Skipping item '{full_name}' with original price {original_price}")
            return None
        
        # Extract image
        img_elem = card.find('img', class_='zmhz363')
        image_url = img_elem['src'] if img_elem and 'src' in img_elem.attrs else None
        # Ignore inline data URLs or non-external image sources
        if not image_url or not image_url.startswith(("http://", "https://")):
            logger.info(f"Skip item '{full_name}' [{country}] due to internal image src ( {image_url})")
            return None
        
        # Extract store
        store_elem = card.find('span', class_='_1fcx6l24')
        store = store_elem.text.strip() if store_elem and store_elem.text else "Unknown Store"
        
        # Extract link
        link_elem = card.find('a', href=True)
        href = link_elem['href'] if link_elem and 'href' in link_elem.attrs else None
        full_url = f"https://www.lyst.com{href}" if href and href.startswith('/') else href if href and href.startswith('http') else None
        
        # Extract unique ID
        product_card_div = card.find('div', class_=lambda x: 'kah5ce0' in x and 'kah5ce2' in x)
        unique_id = product_card_div['id'] if product_card_div and 'id' in product_card_div.attrs else None
        
        # Validate required fields
        required_fields = {
            'name': full_name, 'original_price': original_price, 'sale_price': sale_price,
            'image_url': image_url, 'store': store, 'shoe_link': full_url, 'unique_id': unique_id
        }
        if any(not v for v in required_fields.values()):
            missing_fields = [f for f, v in required_fields.items() if not v]
            logger.warning(f"Missing required fields: {', '.join(missing_fields)}")
            return None
        
        return {
            'name': full_name, 'original_price': original_price, 'sale_price': sale_price,
            'image_url': image_url, 'store': store, 'country': country,
            'shoe_link': full_url, 'unique_id': unique_id
        }
    except Exception as e:
        logger.error(f"Error extracting shoe data: {e}")
        return None

async def scrape_page(url, country, max_scroll_attempts=None):
    soup = await get_soup(url, country, max_scroll_attempts=max_scroll_attempts)
    if not soup: return []
    
    shoe_cards = soup.find_all('div', class_='_693owt3')
    return [data for card in shoe_cards if (data := extract_shoe_data(card, country))]

async def scrape_all_pages(base_url, country, use_pagination=None):
    if use_pagination is None:
        use_pagination = PAGE_SCRAPE
    
    max_scroll_attempts = 10 if use_pagination else 300
    all_shoes, page = [], 1
    
    while True:
        if use_pagination:
            url = base_url['url'] if page == 1 else f"{base_url['url']}&page={page}"
            logger.info(f"Scraping page {page} for country {country} - {base_url['url_name']}")
        else:
            url = base_url['url']
            logger.info(f"Scraping single page for country {country} - {base_url['url_name']}")

        shoes = await scrape_page(url, country, max_scroll_attempts=max_scroll_attempts)
        if not shoes:
            if use_pagination and page < 3:
                logger.error(f"{base_url['url_name']} for {country} Stopped too early. Please check for errors")
                if use_pagination == PAGE_SCRAPE:
                    logger.info(f"Retrying {base_url['url_name']} for {country} with PAGE_SCRAPE={not use_pagination}")
                    return await scrape_all_pages(base_url, country, use_pagination=not use_pagination)
            
            logger.info(f"Total for {country} {base_url['url_name']}: {len(all_shoes)}. Stopped on page {page}")
            break
        all_shoes.extend(shoes)
        
        if not use_pagination:
            break
            
        page += 1
        await asyncio.sleep(1) 
    return all_shoes

# Price and currency conversions
def calculate_sale_percentage(original_price, sale_price, country):
    def parse(p):
        symbol = '€' if country in ('PL', 'IT') else '£' if country == 'GB' else '$'
        p = p.replace(symbol, '').strip()
        p = p.replace(',', '.') if symbol == '€' and (',' in p and '.' not in p) else p.replace(',', '')
        return float(re.sub(r'[^\d.]', '', p) or 0)
    try:
        original, sale = parse(original_price), parse(sale_price)
        return int((1 - sale / original) * 100) if original > 0 else 0
    except Exception:
        return 0

def load_exchange_rates():
    try:
        with open(EXCHANGE_RATES_FILE, 'r') as f:
            data = json.load(f)
        is_fresh = (datetime.now() - datetime.fromisoformat(data['last_update'])).days < 1
        return data['rates'] if is_fresh else update_exchange_rates()
    except (FileNotFoundError, json.JSONDecodeError, KeyError, ValueError):
        return update_exchange_rates()


def update_exchange_rates():
    try:
        resp = requests.get(f"https://v6.exchangerate-api.com/v6/{EXCHANGERATE_API_KEY}/latest/UAH").json()
        rates = {k: resp['conversion_rates'][k] for k in ('EUR', 'USD', 'GBP')}
        with open(EXCHANGE_RATES_FILE, 'w') as f:
            json.dump({'last_update': datetime.now().isoformat(), 'rates': rates}, f)
        return rates
    except Exception as e:
        logger.error(f"Error updating exchange rates: {e}")
        return {'EUR': 1, 'USD': 1, 'GBP': 1}

def convert_to_uah(price, country, exchange_rates, name):
    try:
        currency_map = {
            '€': ('EUR', lambda p: float(p.replace('€','').replace(',', '.').strip())),
            '£': ('GBP', lambda p: float(p.replace('£','').replace(',', '').strip())),
            '$': ('USD', lambda p: float(p.replace('$','').replace(',', '').strip()))
        }
        
        for symbol, (code, parse_fn) in currency_map.items():
            if symbol in price:
                currency, currency_symbol = code, symbol
                amount = parse_fn(price)
                break
        else:
            logger.error(f"Unrecognized currency symbol in price '{price}' for '{name}' country '{country}'")
            return ConversionResult(0, 0, '')

        rate = exchange_rates.get(currency)
        if not rate:
            logger.error(f"Exchange rate not found for currency '{currency}' (country: {country})")
            return ConversionResult(0, 0, '')

        uah_amount = amount / rate
        return ConversionResult(round(uah_amount / 10) * 10, round(1 / rate, 2), currency_symbol)
    except (ValueError, KeyError) as e:
        logger.error(f"Error converting price '{price}' for '{name}' country '{country}': {e}")
        return ConversionResult(0, 0, '')

# Message formatting and sending
def get_sale_emoji(sale_percentage, uah_sale):
    if sale_percentage >= SALE_EMOJI_ROCKET_THRESHOLD: return "🚀🚀🚀"
    if uah_sale < SALE_EMOJI_UAH_THRESHOLD: return "🐚🐚🐚"
    return "🍄🍄🍄"

def build_shoe_message(shoe, sale_percentage, uah_sale, kurs, kurs_symbol, old_sale_price=None, status=None):
    if status is None:  # New item
        sale_emoji = get_sale_emoji(sale_percentage, uah_sale)
        return (
            f"{sale_emoji}  New item  {sale_emoji}\n{shoe['name']}\n\n"
            f"💀 Prices : <s>{shoe['original_price']}</s>  <b>{shoe['sale_price']}</b>  <i>(Sale: <b>{sale_percentage}%</b>)</i>\n"
            f"🤑 Grivniki : <b>{uah_sale} UAH </b>\n"
            f"🧊 Kurs : {kurs_symbol} {kurs} \n"
            f"🔗 Store : <a href='{shoe['shoe_link']}'>{shoe['store']}</a>\n"
            f"🌍 Country : {shoe['country']}"
        )
    return (
        f"💎💎💎 {status} 💎💎💎 \n{shoe['name']}:\n\n"
        f"💀 Prices : <s>{shoe['original_price']}</s>  <s>{old_sale_price}</s>  <b>{shoe['sale_price']}</b>  <i>(Sale: <b>{sale_percentage}%</b>)</i> \n"
        f"🤑 Grivniki : {uah_sale} UAH\n"
        f"📉 Lowest price : {shoe['lowest_price']} ({shoe['lowest_price_uah']} UAH)\n"
        f"🧊 Kurs : {kurs_symbol} {kurs} \n"
        f"🔗 Store : <a href='{shoe['shoe_link ']}'>{shoe['store']}</a>\n"
        f"🌍 Country : {shoe['country']}"
    )

async def send_telegram_message(bot_token, chat_id, message, image_url=None, uah_price=None, sale_percentage=None, max_retries=3):
    bot = Bot(token=bot_token)
    for attempt in range(max_retries):
        try:
            if image_url and image_url.startswith(('http://', 'https://')):
                if uah_price is not None and sale_percentage is not None:
                    img_byte_arr = process_image(image_url, uah_price, sale_percentage)
                    await bot.send_photo(chat_id=chat_id, photo=img_byte_arr, caption=message, parse_mode='HTML')
                else:
                    await bot.send_photo(chat_id=chat_id, photo=image_url, caption=message, parse_mode='HTML')
            else:
                await bot.send_message(chat_id=chat_id, text=message, parse_mode='HTML')
            return True
        except RetryAfter as e:
            logger.warning(f"Rate limited. Sleeping for {e.retry_after} seconds")
            await asyncio.sleep(e.retry_after)
        except TimedOut:
            logger.warning(f"Request timed out on attempt {attempt + 1}")
            await asyncio.sleep(3 * (attempt + 1))
        except Exception as e:
            logger.error(f"Error sending Telegram message (attempt {attempt + 1}): {e}")
            if attempt == max_retries - 1:
                logger.error(f"Failed to send Telegram message after {max_retries} attempts")
                return False
            await asyncio.sleep(2 * (attempt + 1))
    return False

# Processing functions
def filter_duplicates(shoes, exchange_rates):
    filtered_shoes, grouped_shoes = [], defaultdict(list)
    for shoe in shoes:
        grouped_shoes[f"{shoe['name']}_{shoe['unique_id']}"] .append(shoe)

    for group in grouped_shoes.values():
        # Deduplicate within same country: prefer item with valid image_url
        country_map = {}
        for shoe in group:
            country = shoe['country']
            if country not in country_map:
                country_map[country] = shoe
            else:
                existing = country_map[country]
                existing_img = existing.get('image_url')
                new_img = shoe.get('image_url')
                
                existing_has_img = existing_img and existing_img.startswith(('http', 'https'))
                new_has_img = new_img and new_img.startswith(('http', 'https'))
                
                if not existing_has_img and new_has_img:
                    country_map[country] = shoe
        
        group = list(country_map.values())

        if len(group) == 1:
            filtered_shoes.append(group[0])
            continue
        group.sort(key=lambda x: COUNTRY_PRIORITY.index(x['country']) if x['country'] in COUNTRY_PRIORITY else len(COUNTRY_PRIORITY))
        for shoe in group:
            shoe['uah_price'] = convert_to_uah(shoe['sale_price'], shoe['country'], exchange_rates, shoe['name']).uah_amount
        base = group[0]
        replacement = next((s for s in group[1:] if base['uah_price'] - s['uah_price'] >= 200), None)
        filtered_shoes.append(replacement or base)
    return filtered_shoes

async def process_shoe(shoe, old_data, message_queue, exchange_rates):
    key = f"{shoe['name']}_{shoe['unique_id']}"
    if await is_shoe_processed(key): return

    # Calculate sale details
    sale_percentage = calculate_sale_percentage(shoe['original_price'], shoe['sale_price'], shoe['country'])
    sale_exchange_data = convert_to_uah(shoe['sale_price'], shoe['country'], exchange_rates, shoe['name'])
    kurs, uah_sale, kurs_symbol = sale_exchange_data.exchange_rate, sale_exchange_data.uah_amount, sale_exchange_data.currency_symbol

    # Handle new shoe
    if key not in old_data:
        shoe.update({
            'lowest_price': shoe['sale_price'],
            'lowest_price_uah': uah_sale,
            'uah_price': uah_sale,
            'active': True
        })
        message = build_shoe_message(shoe, sale_percentage, uah_sale, kurs, kurs_symbol)
        message_id = await message_queue.add_message(shoe['base_url']['telegram_chat_id'], message, shoe['image_url'], uah_sale, sale_percentage)
        while not message_queue.is_message_sent(message_id):
            await asyncio.sleep(1)
        await mark_shoe_processed(key)
        old_data[key] = shoe
        # Save individual shoe instead of entire dataset
        await save_shoe_data_bulk([dict(shoe, key=key)])
    else:
        # Update existing shoe
        old_shoe = old_data[key]
        old_sale_price = old_shoe['sale_price']
        old_sale_country = old_shoe['country']
        old_uah = old_shoe.get('uah_price') or convert_to_uah(old_sale_price, old_sale_country, exchange_rates, shoe['name']).uah_amount
        shoe['uah_price'] = uah_sale
        lowest_price_uah = old_shoe.get('lowest_price_uah') or old_uah

        # Update lowest price if needed
        if uah_sale < lowest_price_uah:
            shoe['lowest_price'], shoe['lowest_price_uah'] = shoe['sale_price'], uah_sale
        else:
            shoe['lowest_price'], shoe['lowest_price_uah'] = old_shoe['lowest_price'], lowest_price_uah
        
        shoe['active'] = True
        old_data[key] = shoe
        # Save individual shoe instead of entire dataset
        await save_shoe_data_bulk([dict(shoe, key=key)])

async def process_all_shoes(all_shoes, old_data, message_queue, exchange_rates):
    new_shoe_count = 0
    semaphore = asyncio.Semaphore(9)  # Reduce concurrency to prevent database locks
    total_items = len(all_shoes)

    async def process_single_shoe(i, shoe):
        nonlocal new_shoe_count
        async with semaphore:  # Limit concurrency
            try:
                country, name, unique_id = shoe['country'], shoe['name'], shoe['unique_id']
                key = f"{name}_{unique_id}"
                sale_percentage = calculate_sale_percentage(shoe['original_price'], shoe['sale_price'], country)
                
                if sale_percentage < shoe['base_url']['min_sale']: return

                # Get final link or use existing one
                if key not in old_data:
                    shoe['shoe_link'] = await get_final_clear_link(shoe['shoe_link'], semaphore, name, country, i, total_items)
                    new_shoe_count += 1
                else:
                    shoe['shoe_link'] = old_data[key]['shoe_link']
                
                await process_shoe(shoe, old_data, message_queue, exchange_rates)
            except Exception as e:
                logger.error(f"Error processing shoe {shoe.get('name', 'unknown')}: {e}")
                logger.error(traceback.format_exc())

    # Process shoes in smaller batches to reduce database contention
    batch_size = 10
    for i in range(0, len(all_shoes), batch_size):
        batch = all_shoes[i:i + batch_size]
        await asyncio.gather(*[process_single_shoe(i + j, shoe) for j, shoe in enumerate(batch)])
        # Small delay between batches to prevent overwhelming the database
        await asyncio.sleep(0.1)
    
    logger.info(f"Processed {new_shoe_count} new shoes in total")

    # Handle removed shoes in batches
    current_shoes = {f"{shoe['name']}_{shoe['unique_id']}" for shoe in all_shoes}
    removed_shoes = [dict(shoe, key=k, active=False) for k, shoe in old_data.items() if k not in current_shoes and shoe.get('active', True)]
    for s in removed_shoes:
        old_data[s['key']]['active'] = False
    if removed_shoes:
        await save_shoe_data_bulk(removed_shoes)

async def process_url(base_url, countries, exchange_rates):
    all_shoes = []
    country_results = await asyncio.gather(*(scrape_all_pages(base_url, c) for c in countries))
    for country, result in zip(countries, country_results):
        for shoe in result:
            if isinstance(shoe, dict):
                shoe['base_url'] = base_url
                all_shoes.append(shoe)
            else:
                logger.error(f"Unexpected item data type for {country}: {type(shoe)}")
        special_logger.info(f"Found {len(result)} items for {country} - {base_url['url_name']}")
    return all_shoes

# Utility functions
def print_statistics():
    special_logger.stat(f"Max wait time for initial URL change: {max_wait_times['url_changes']:.2f} seconds")
    special_logger.stat(f"Max wait time for final URL change: {max_wait_times['final_url_changes']:.2f} seconds")
        
def print_link_statistics():
    if 'steps' in link_statistics:
        special_logger.stat("Final URL obtained at the following steps:")
        total_final_urls = sum(info['final_url_obtained'] for info in link_statistics['steps'].values())

        for step_name, info in link_statistics['steps'].items():
            count, final_url_count = info['count'], info['final_url_obtained']
            success_rate = (final_url_count / count) * 100 if count > 0 else 0
            percentage_of_total = (final_url_count / total_final_urls) * 100 if total_final_urls > 0 else 0
            special_logger.stat(f"{step_name}: {final_url_count}/{count} final URLs obtained ({success_rate:.2f}% success rate), {percentage_of_total:.2f}% of total final URLs")
                    
def center_text(text, width, fill_char=' '): return text.center(width, fill_char)

# Main application
async def main():
    global LIVE_MODE
    # Initialize and start message queue
    message_queue = TelegramMessageQueue(TELEGRAM_BOT_TOKEN)
    asyncio.create_task(message_queue.process_queue())

    terminal_width = shutil.get_terminal_size().columns
    bot_version = f"Grotesk bot v.{BOT_VERSION}"
    print(
        Fore.GREEN + '-' * terminal_width + Style.RESET_ALL + '\n' +
        Fore.CYAN + Style.BRIGHT + bot_version.center(terminal_width) + Style.RESET_ALL + '\n' +
        Fore.GREEN + '-' * terminal_width + Style.RESET_ALL
    )

    if ASK_FOR_LIVE_MODE:
        LIVE_MODE = input("Enter 'live' to enable live mode, or press Enter to continue in headless mode: ").strip().lower() == 'live'
    if LIVE_MODE:
        special_logger.good("Live mode enabled")

    try:
        while True:
            try:
                # Load data and exchange rates
                old_data = await load_shoe_data()
                exchange_rates = load_exchange_rates()

                
                # Also run OLX scraper this cycle
                await run_olx_scraper()

                # Process all URLs concurrently
                url_tasks = [process_url(base_url, COUNTRIES, exchange_rates) for base_url in BASE_URLS]
                url_results = await asyncio.gather(*url_tasks)
                
                # Combine results and filter duplicates
                all_shoes = []
                for result in url_results:
                    all_shoes.extend(result)
                    
                unfiltered_len = len(all_shoes)
                all_shoes = filter_duplicates(all_shoes, exchange_rates)
                special_logger.stat(f"Removed {unfiltered_len - len(all_shoes)} duplicates")
                
                # Process all shoes
                await process_all_shoes(all_shoes, old_data, message_queue, exchange_rates)

                # Print statistics and wait for next cycle
                print_statistics()
                print_link_statistics()
                logger.info("Sleeping for 1 hour before next check")
                await asyncio.sleep(3600)
                
            except KeyboardInterrupt:
                logger.info("Script terminated by user")
                break
            except Exception as e:
                logger.error(f"An unexpected error occurred in main loop: {e}")
                logger.error(traceback.format_exc())
                logger.info("Waiting for 60 minutes before retrying")
                await asyncio.sleep(3600)
    finally:
        pass  # Removed application.stop() as we're no longer using telegram.ext.Application

if __name__ == "__main__":
    create_tables()  # Create tables at startup instead of creating them just before using
    asyncio.run(main())
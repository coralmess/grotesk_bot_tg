import json
import time
import asyncio
import logging
import colorama
import subprocess
import shutil
import traceback
import urllib.parse
import re
import html
import io
import uuid
import requests
from telegram.ext import CommandHandler, Application
from telegram.constants import ParseMode
from collections import defaultdict, namedtuple
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from telegram import Bot
from telegram.error import RetryAfter, TimedOut
from config import TELEGRAM_BOT_TOKEN, EXCHANGERATE_API_KEY, BASE_URLS
from colorama import Fore, Back, Style
from PIL import Image, ImageDraw, ImageFont

colorama.init(autoreset=True)

BOT_VERSION = "3.6.5.1"
last_git_pull_time = None


class CompactGroupedMessageHandler(logging.Handler):
    def __init__(self, timeout=5):
        super().__init__()
        self.message_counts = defaultdict(lambda: {'count': 0, 'last_time': 0})
        self.timeout = timeout

    def emit(self, record):
        current_time = time.time()
        msg = self.format(record)
        base_msg = msg[:23]
        content_msg = msg[23:]

        if content_msg in self.message_counts:
            last_time = self.message_counts[content_msg]['last_time']
            if current_time - last_time < self.timeout:
                self.message_counts[content_msg]['count'] += 1
                self.message_counts[content_msg]['last_time'] = current_time
                return

        count = self.message_counts[content_msg]['count']
        if count > 0:
            print(f"{base_msg}{content_msg} ({count + 1})")
        else:
            print(msg)

        self.message_counts[content_msg] = {'count': 0, 'last_time': current_time}

class ColoredFormatter(logging.Formatter):
    COLORS = {
        'DEBUG': Fore.CYAN,
        'INFO': Fore.WHITE,
        'WARNING': Fore.YELLOW,
        'ERROR': Fore.RED,
        'CRITICAL': Fore.RED + Back.WHITE,
        'STAT': Fore.MAGENTA,
        'GOOD': Fore.GREEN,
        'LIGHTBLUE_INFO': Fore.LIGHTBLUE_EX
    }

    def format(self, record):
        log_color = self.COLORS.get(record.levelname, Fore.WHITE)
        timestamp = Fore.LIGHTBLACK_EX + self.formatTime(record, self.datefmt) + Style.RESET_ALL
        message = log_color + record.getMessage() + Style.RESET_ALL
        return f"{timestamp}     {message}"
    
class TelegramMessageQueue:
    def __init__(self, bot_token):
        self.queue = asyncio.Queue()
        self.bot_token = bot_token
        self.pending_messages = {}  # Track messages waiting to be sent

    async def add_message(self, chat_id, message, image_url=None, uah_price=None, sale_percentage=None):
        message_id = str(uuid.uuid4())
        self.pending_messages[message_id] = False  # False means not sent yet
        await self.queue.put((message_id, chat_id, message, image_url, uah_price, sale_percentage))
        return message_id

    async def process_queue(self):
        while True:
            message_id, chat_id, message, image_url, uah_price, sale_percentage = await self.queue.get()
            success = await send_telegram_message(self.bot_token, chat_id, message, image_url, uah_price, sale_percentage)
            
            if success:
                self.pending_messages[message_id] = True
            else:
                # If sending fails, put the message back in the queue
                await self.queue.put((message_id, chat_id, message, image_url, uah_price, sale_percentage))
            
            await asyncio.sleep(1)  # Delay to prevent hitting rate limit
    
    def is_message_sent(self, message_id):
        return self.pending_messages.get(message_id, False)

def setup_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    formatter = ColoredFormatter('%(asctime)s', datefmt='%d.%m %H:%M:%S')

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger



logger = setup_logger()

class SpecialLogger:
    @staticmethod
    def stat(message):
        logger.log(35, message)

    @staticmethod
    def good(message):
        logger.log(25, message)
        
    @staticmethod
    def info(message):
        logger.log(22, message)

special_logger = SpecialLogger()

processed_shoes = set()
TelegramMessage = namedtuple('TelegramMessage', ['chat_id', 'message', 'image_url'])
ConversionResult = namedtuple('ConversionResult', ['uah_amount', 'exchange_rate', 'currency_symbol'])

# Add these custom log levels
logging.addLevelName(35, "STAT")
logging.addLevelName(25, "GOOD")
logging.addLevelName(22, "LIGHTBLUE_INFO")


LIVE_MODE = False  # True to enable live mode by default
ASK_FOR_LIVE_MODE = False  # False to skip asking for live mode at startup

# Files to store data
SHOE_DATA_FILE = 'shoe_data.json'
EXCHANGE_RATES_FILE = 'exchange_rates.json'

# Countries to scrape
COUNTRIES = ['IT', 'PL', 'US', 'GB']

max_wait_times = {'url_changes': 0, 'final_url_changes': 0}
store_statistics = defaultdict(lambda: {'success': 0, 'total': 0})
link_statistics = {
    'lyst_track_lead': {'success': 0, 'fail': 0, 'fail_links': []},
    'click_here': {'success': 0, 'fail': 0, 'fail_links': []},
    'other_failures': {'count': 0, 'links': []}
}

def get_git_info():
    global last_git_pull_time
    if last_git_pull_time is None:
        try:
            git_log = subprocess.check_output(['git', 'log', '-1', '--format=%cd'], universal_newlines=True).strip()
            last_git_pull_time = datetime.strptime(git_log, '%a %b %d %H:%M:%S %Y %z')
        except subprocess.CalledProcessError:
            last_git_pull_time = "Unknown"
    return last_git_pull_time

async def ver_command(update):
    git_pull_time = get_git_info()
    response = f"Bot version: {BOT_VERSION}\nLast git pull: {git_pull_time}"
    await update.message.reply_text(response)

def clean_link_for_display(link):
    # Remove 'http://', 'https://', and 'www.' from the beginning of the link
    cleaned_link = re.sub(r'^(https?://)?(www\.)?', '', link)
    # Truncate to 22 characters and add '...' if longer
    return (cleaned_link[:22] + '...') if len(cleaned_link) > 25 else cleaned_link

async def linkstat_command(update):
    stats_message = "Link Processing Statistics:\n\n"
    
    for step, stats in link_statistics.items():
        if step != 'other_failures':
            success_rate = (stats['success'] / (stats['success'] + stats['fail'])) * 100 if (stats['success'] + stats['fail']) > 0 else 0
            stats_message += f"{step}:\n"
            stats_message += f"  Success rate: {success_rate:.2f}%\n"
            stats_message += f"  Successful: {stats['success']}\n"
            stats_message += f"  Failed: {stats['fail']}\n"
            if stats['fail'] > 0:
                stats_message += "  Failed links (up to 5):\n"
                for link in stats['fail_links'][:5]:
                    display_link = clean_link_for_display(link)
                    stats_message += f"    <a href='{html.escape(link)}'>{html.escape(display_link)}</a>\n"
        else:
            stats_message += f"Other failures: {stats['count']}\n"
            if stats['count'] > 0:
                stats_message += "  Failed links (up to 5):\n"
                for link in stats['links'][:5]:
                    display_link = clean_link_for_display(link)
                    stats_message += f"    <a href='{html.escape(link)}'>{html.escape(display_link)}</a>\n"
        stats_message += "\n"
    
    # If the message is too long, split it into multiple messages
    max_message_length = 4096  # Telegram's max message length
    messages = []
    while len(stats_message) > 0:
        if len(stats_message) <= max_message_length:
            messages.append(stats_message)
            break
        else:
            split_index = stats_message.rfind('\n', 0, max_message_length)
            if split_index == -1:
                split_index = max_message_length
            messages.append(stats_message[:split_index])
            stats_message = stats_message[split_index:].lstrip()

    for message in messages:
        await update.message.reply_text(message, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        
def process_image(image_url, uah_price, sale_percentage):
    # Download the image
    response = requests.get(image_url)
    try:
        img = Image.open(io.BytesIO(response.content))

        # Calculate font size based on image dimensions
        width, height = img.size
        font_size = int(min(width, height) * 0.055)

        # Create draw object
        draw = ImageDraw.Draw(img)

        # Load fonts
        try:
            regular_font = ImageFont.truetype("SFPro-Bold.ttf", font_size)
        except IOError:
            try:
                regular_font = ImageFont.truetype("arialbd.ttf", font_size)
            except IOError:
                regular_font = ImageFont.load_default()

        # Calculate text sizes
        price_text = f"{uah_price} UAH"
        sale_text = f"-{sale_percentage}%"
        price_bbox = draw.textbbox((0, 0), price_text, font=regular_font)
        sale_bbox = draw.textbbox((0, 0), sale_text, font=regular_font)

        # Calculate text heights
        price_height = price_bbox[3] - price_bbox[1]
        sale_height = sale_bbox[3] - sale_bbox[1]
        max_text_height = max(price_height, sale_height)

        # Calculate additional space needed
        padding = 5
        additional_height = max_text_height + (padding * 2)

        # Create a new image with additional space at the bottom
        new_img = Image.new('RGBA', (width, height + additional_height), (0, 0, 0, 0))
        new_img.paste(img, (0, 0))

        # Create a gradient overlay for the bottom area
        overlay = Image.new('RGBA', (width, additional_height), (0, 0, 0, 0))
        new_img.paste(overlay, (0, height), overlay)

        # Update draw object for the new image
        draw = ImageDraw.Draw(new_img)

        # Calculate vertical center for text in the bottom area
        text_y = height + padding + ( max_text_height // 2 )

        # Add price text (left-aligned)
        price_x = 30
        draw.text((price_x, text_y), price_text, font=regular_font, fill=(22,22,24), anchor="lm")

        # Add sale percentage (right-aligned)
        sale_x = width - 30
        draw.text((sale_x, text_y), sale_text, font=regular_font, fill=(255,59,48), anchor="rm")  # Apple's red color

        # Save the image to a bytes buffer
        img_byte_arr = io.BytesIO()
        new_img.save(img_byte_arr, format='PNG')
        img_byte_arr.seek(0)

        return img_byte_arr
    finally:
        # Ensure the response is closed
        response.close()

def get_driver():
    options = Options()
    options.headless = True
    # options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    return webdriver.Firefox(options=options)

async def get_page_content(url, country):
    async with async_playwright() as p:
        browser = await p.firefox.launch(headless=not LIVE_MODE)
        context = await browser.new_context()
        
        await context.add_cookies([{
            'name': 'country',
            'value': country,
            'domain': '.lyst.com',
            'path': '/'
        }])
        
        page = await context.new_page()
        
        await page.goto(url)
        
        await scroll_page(page)
        
        try:
            await page.wait_for_selector('._693owt3', timeout=10000)
        except:
            await browser.close()
            return None
        
        content = await page.content()
        
        await browser.close()
        return content

async def scroll_page(page):
    SCROLL_PAUSE_TIME = 1 if not LIVE_MODE else 10
    SCROLL_STEP = 600
    MAX_SCROLL_ATTEMPTS = 100

    last_height = await page.evaluate("document.body.scrollHeight")
    total_scrolled = 0
    scroll_attempts = 0

    while scroll_attempts < MAX_SCROLL_ATTEMPTS:
        await page.evaluate(f"window.scrollBy(0, {SCROLL_STEP})")
        total_scrolled += SCROLL_STEP
        
        await asyncio.sleep(SCROLL_PAUSE_TIME)

        new_height = await page.evaluate("document.body.scrollHeight")
        
        if total_scrolled > new_height:
            break
        
        if new_height > last_height:
            scroll_attempts = 0
        else:
            scroll_attempts += 1

        last_height = new_height

async def get_soup(url, country, max_retries=3):
    for attempt in range(max_retries):
        try:
            content = await get_page_content(url, country)
            if content:
                return BeautifulSoup(content, 'html.parser')
            return None
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Failed to get soup (attempt {attempt + 1}/{max_retries}). Retrying...")
                await asyncio.sleep(5)
            else:
                logger.error(f"Failed to get soup for {url}")
                raise

def load_shoe_data():
    try:
        with open(SHOE_DATA_FILE, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def save_shoe_data(data):
    with open(SHOE_DATA_FILE, 'w') as f:
        json.dump(data, f)
    special_logger.good("Shoe data saved successfully")
    
def is_lyst_domain(url):
    parsed_url = urllib.parse.urlparse(url)
    return parsed_url.netloc.endswith('lyst.com')
    
def extract_price(price_str):
    # Remove all non-digit characters except for the decimal point
    price_num = re.sub(r'[^\d.]', '', price_str)
    try:
        return float(price_num)
    except ValueError:
        return 0

async def get_final_clear_link(initial_url, semaphore, item_name, country, current_item, total_items, store):
    async with semaphore:
        logger.info(f"Processing final link for {item_name} | Country: {country} | Progress: {current_item}/{total_items}")
        async with async_playwright() as p:
            browser = await p.firefox.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()
            
            try:
                await page.goto(initial_url)
                
                start_time = time.time()
                await page.wait_for_url(lambda url: url != initial_url, timeout=20000)
                wait_time = time.time() - start_time
                max_wait_times['url_changes'] = max(max_wait_times['url_changes'], wait_time)
                
                await asyncio.sleep(5)
                
                current_url = page.url

                def extract_embedded_url(url):
                    parsed = urllib.parse.urlparse(url)
                    query_params = urllib.parse.parse_qs(parsed.query)
                    for param in ['URL', 'murl', 'destination', 'url']:
                        if param in query_params:
                            return urllib.parse.unquote(query_params[param][0])
                    return url

                # Check for embedded URL at each step
                current_url = extract_embedded_url(current_url)

                if "lyst.com" in current_url and "return" in current_url:
                    await page.goto(current_url)
                    await page.wait_for_load_state('networkidle')
                    buy_button = await page.wait_for_selector("//a[contains(text(), 'Buy from')]", timeout=10000)
                    if buy_button:
                        await buy_button.click()
                        await page.wait_for_url(lambda url: "lyst.com" not in url, timeout=20000)
                        current_url = page.url
                        current_url = extract_embedded_url(current_url)
                
                if "lyst.com/track/lead" in current_url:
                    link_statistics['lyst_track_lead']['success'] += 1
                    try:
                        click_here_button = await page.wait_for_selector("//a[contains(text(), 'Click here')]", timeout=10000)
                        await click_here_button.click()
                        link_statistics['click_here']['success'] += 1
                        
                        start_time = time.time()
                        await page.wait_for_url(lambda url: url != current_url, timeout=20000)
                        wait_time = time.time() - start_time
                        max_wait_times['final_url_changes'] = max(max_wait_times['final_url_changes'], wait_time)
                        current_url = page.url
                        current_url = extract_embedded_url(current_url)
                    except:
                        link_statistics['click_here']['fail'] += 1
                        link_statistics['click_here']['fail_links'].append(current_url)
                else:
                    if is_lyst_domain(current_url):
                        link_statistics['lyst_track_lead']['fail'] += 1
                        link_statistics['lyst_track_lead']['fail_links'].append(current_url)
                    else:
                        embedded_url = extract_embedded_url(current_url)
                        if embedded_url != current_url:
                            current_url = embedded_url
                            link_statistics['lyst_track_lead']['success'] += 1
                
                final_url = current_url
                
                # Ensure the final_url is properly unquoted
                final_url = urllib.parse.unquote(final_url)
                
                logger.info(f"Final link obtained for: {item_name}")
                
                store_statistics[store]['total'] += 1
                store_statistics[store]['success'] += 1
                
                return final_url
            
            except Exception as e:
                link_statistics['other_failures']['count'] += 1
                link_statistics['other_failures']['links'].append(initial_url)
                
                store_statistics[store]['total'] += 1
                
                return initial_url
            finally:
                await browser.close()

def extract_shoe_data(card, country):
    try:
        if card is None:
            logger.warning("Received None card in extract_shoe_data")
            return None

        # Name extraction
        name_elements = card.find_all('span', class_=lambda x: x and 'vjlibs5' in x)
        if not name_elements:
            logger.warning("No name elements found")
            return None
        full_name = ' '.join([elem.text.strip() for elem in name_elements if elem and elem.text])
        
        if 'Giuseppe Zanotti' in full_name:
            return None
        
        # Price extraction
        price_div = card.find('div', class_='ducdwf0')
        if not price_div:
            logger.warning("Price div not found")
            return None
        
        # Updated selectors for price elements
        original_price_elem = price_div.find('div', class_=lambda x: x and '_1b08vvhor' in x and 'vjlibs1' in x)
        sale_price_elem = price_div.find('div', class_=lambda x: x and '_1b08vvh1w' in x and 'vjlibs2' in x)
        
        if original_price_elem == sale_price_elem:
            return None
        
        if not original_price_elem or not sale_price_elem:
            logger.warning("Price elements not found")
            return None
        
        original_price = original_price_elem.text.strip() if original_price_elem.text else "N/A"
        sale_price = sale_price_elem.text.strip() if sale_price_elem.text else "N/A"
        
        original_price_value = extract_price(original_price)

        if original_price_value < 115:
            logger.info(f"Skipping item '{full_name}' with original price {original_price}")
            return None
        
        # Image URL extraction
        img_elem = card.find('img', class_='zmhz363')
        image_url = img_elem['src'] if img_elem and 'src' in img_elem.attrs else None
        
        # Store extraction
        store_element = card.find('span', class_='_1fcx6l24')
        store = store_element.text.strip() if store_element and store_element.text else "Unknown Store"
        
        # Link extraction
        link_element = card.find('a', href=True)
        if link_element and 'href' in link_element.attrs:
            href = link_element['href']
            full_url = f"https://www.lyst.com{href}" if href.startswith('/') else href if href.startswith('http') else None
        else:
            full_url = None
        
        # Unique ID extraction
        product_card_div = card.find('div', class_=lambda x: x and 'kah5ce0' in x and 'kah5ce2' in x)
        unique_id = product_card_div['id'] if product_card_div and 'id' in product_card_div.attrs else None
        
        required_fields = {
            'name': full_name,
            'original_price': original_price,
            'sale_price': sale_price,
            'image_url': image_url,
            'store': store,
            'shoe_link': full_url,
            'unique_id': unique_id
        }
        
        missing_fields = [field for field, value in required_fields.items() if not value]
        
        if missing_fields:
            logger.warning(f"Missing required fields: {', '.join(missing_fields)}")
            return None

        return {
            'name': full_name,
            'original_price': original_price,
            'sale_price': sale_price,
            'image_url': image_url,
            'store': store,
            'country': country,
            'shoe_link': full_url,
            'unique_id': unique_id
        }
    except Exception as e:
        logger.error(f"Error extracting shoe data: {e}")
        return None

async def scrape_page(url, country):
    soup = await get_soup(url, country)
    if not soup:
        return []
    
    shoe_cards = soup.find_all('div', class_='_693owt3')
    shoes = []
    for card in shoe_cards:
        data = extract_shoe_data(card, country)
        if data:
            shoes.append(data)
    return shoes

async def scrape_all_pages(base_url, country):
    all_shoes = []
    page = 1
    while True:
        url = f"{base_url['url']}&page={page}"
        logger.info(f"Scraping page {page} for country {country} - {base_url['url_name']}")
        shoes = await scrape_page(url, country)
        if not shoes:
            logger.info(f"Total items for {country}  {base_url['url_name']}: {len(all_shoes)}.  Stopping")
            break
        all_shoes.extend(shoes)
        page += 1
        await asyncio.sleep(1) 
    return all_shoes

def load_shoe_data():
    try:
        with open(SHOE_DATA_FILE, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def save_shoe_data(data):
    with open(SHOE_DATA_FILE, 'w') as f:
        json.dump(data, f)

def calculate_sale_percentage(original_price, sale_price, country):
    try:
        if country in ['PL', 'IT']:
            original = float(original_price.replace('€', '').replace(',', '.'))
            sale = float(sale_price.replace('€', '').replace(',', '.'))
        elif country == 'GB':
            original = float(original_price.replace('£', '').replace(',', ''))
            sale = float(sale_price.replace('£', '').replace(',', ''))
        else:
            original = float(original_price.replace('$', '').replace(',', ''))
            sale = float(sale_price.replace('$', '').replace(',', ''))
        if original > 0:
            return int((1 - sale / original) * 100)
        else:
            return 0
    except ValueError:
        return 0

async def send_telegram_message(bot_token, chat_id, message, image_url=None, uah_price=None, sale_percentage=None, max_retries=3):
    bot = Bot(token=bot_token)
    for attempt in range(max_retries):
        try:
            if image_url and image_url.startswith(('http://', 'https://')):
                if uah_price is not None and sale_percentage is not None:
                    # Process the image
                    img_byte_arr = process_image(image_url, uah_price, sale_percentage)
                    result = await bot.send_photo(chat_id=chat_id, photo=img_byte_arr, caption=message, parse_mode='HTML')
                else:
                    result = await bot.send_photo(chat_id=chat_id, photo=image_url, caption=message, parse_mode='HTML')
            else:
                result = await bot.send_message(chat_id=chat_id, text=message, parse_mode='HTML')
            
            return True

        except RetryAfter as e:
            # Handle rate limiting
            sleep_time = e.retry_after
            logger.warning(f"Rate limited. Sleeping for {sleep_time} seconds")
            await asyncio.sleep(sleep_time)
            continue

        except TimedOut:
            # Handle timeouts
            logger.warning(f"Request timed out on attempt {attempt + 1}")
            await asyncio.sleep(3 * (attempt + 1))  # Exponential backoff
            continue

        except Exception as e:
            logger.error(f"Error sending Telegram message (attempt {attempt + 1}): {e}")
            if attempt == max_retries - 1:
                logger.error(f"Failed to send Telegram message after {max_retries} attempts")
                return False
            await asyncio.sleep(2 * (attempt + 1))  # Exponential backoff

    return False


def load_exchange_rates():
    try:
        with open(EXCHANGE_RATES_FILE, 'r') as f:
            data = json.load(f)
        if (datetime.now() - datetime.fromisoformat(data['last_update'])).days < 1:
            return data['rates']
    except (FileNotFoundError, json.JSONDecodeError, KeyError):
        pass
    
    return update_exchange_rates()

def filter_duplicates(shoes, exchange_rates):
    priority = ["PL", "US", "IT", "GB"]
    filtered_shoes = []
    grouped_shoes = defaultdict(list)

    for shoe in shoes:
        key = f"{shoe['name']}_{shoe['unique_id']}"
        grouped_shoes[key].append(shoe)

    for key, group in grouped_shoes.items():
        if len(group) == 1:
            filtered_shoes.append(group[0])
        else:
            group.sort(key=lambda x: priority.index(x['country']))
            
            for shoe in group:
                convertion_data = convert_to_uah(shoe['sale_price'], shoe['country'], exchange_rates, shoe['name'])
                shoe['uah_price'] = convertion_data.uah_amount

            base_shoe = group[0]
            filtered_shoes.append(base_shoe)

            for shoe in group[1:]:
                price_difference = base_shoe['uah_price'] - shoe['uah_price']
                if price_difference >= 200:  
                    filtered_shoes.pop()  
                    filtered_shoes.append(shoe)
                    break 

    return filtered_shoes

def update_exchange_rates():
    try:
        import requests
        response = requests.get(f"https://v6.exchangerate-api.com/v6/{EXCHANGERATE_API_KEY}/latest/UAH")
        data = response.json()
        rates = {
            'EUR': data['conversion_rates']['EUR'],
            'USD': data['conversion_rates']['USD'],
            'GBP': data['conversion_rates']['GBP']
        }
        with open(EXCHANGE_RATES_FILE, 'w') as f:
            json.dump({
                'last_update': datetime.now().isoformat(),
                'rates': rates
            }, f)
        return rates
    except Exception as e:
        logger.error(f"Error updating exchange rates: {e}")
        return {'EUR': 1, 'USD': 1, 'GBP': 1}


def convert_to_uah(price, country, exchange_rates, name):
    try:
        # Detect currency symbol and remove it
        if '€' in price:
            currency = 'EUR'
            currency_symbol = '€'
            amount = float(price.replace('€', '').replace(',', '.').strip())
        elif '£' in price:
            currency = 'GBP'
            currency_symbol = '£'
            amount = float(price.replace('£', '').replace(',', '').strip())
        elif '$' in price:
            currency = 'USD'
            currency_symbol = '$'
            amount = float(price.replace('$', '').replace(',', '').strip())
        else:
            logger.error(f"Unrecognized currency symbol in price '{price}' for '{name}' country '{country}'")
            return ConversionResult(0, 0, '')

        if currency in exchange_rates:
            rate = exchange_rates[currency]
        else:
            logger.error(f"Exchange rate not found for currency '{currency}' (country: {country})")
            return ConversionResult(0, 0, '')
        
        uah_amount = amount / rate
        rounded_uah_amount = round(uah_amount / 10) * 10
        exchange_rate = round(1 / rate, 2)
        
        return ConversionResult(rounded_uah_amount, exchange_rate, currency_symbol)
    
    except ValueError as e:
        logger.error(f"Error converting price '{price}' for '{name}' country '{country}': {e}")
        return ConversionResult(0, 0, '')
    except KeyError as e:
        logger.error(f"Missing exchange rate for currency '{currency}': {e}")
        return ConversionResult(0, 0, '')
    

def get_sale_emoji(sale_percentage, uah_sale):
    if sale_percentage >= 75 :
        return "🚀🚀🚀" 
    if uah_sale < 2600: 
        return "🐚🐚🐚"

    return "🍄🍄🍄"

async def process_shoe(shoe, old_data, message_queue, exchange_rates):
    global processed_shoes
    country = shoe['country']
    name = shoe['name']
    unique_id = shoe['unique_id']
    key = f"{name}_{unique_id}"
    
    # Check if this shoe has already been processed in this run
    if key in processed_shoes:
        return
    
    sale_percentage = calculate_sale_percentage(shoe['original_price'], shoe['sale_price'], country)
    sale_exchange_data = convert_to_uah(shoe['sale_price'], country, exchange_rates, shoe['name'])
    original_exchange_data = convert_to_uah(shoe['original_price'], country, exchange_rates, shoe['name'])
    uah_orig = original_exchange_data.uah_amount
    kurs = sale_exchange_data.exchange_rate
    uah_sale = sale_exchange_data.uah_amount
    kurs_symbol = sale_exchange_data.currency_symbol

    if key not in old_data:
        shoe['lowest_price'] = shoe['sale_price']
        shoe['lowest_price_uah'] = uah_sale
        shoe['uah_price'] = uah_sale
        shoe['active'] = True
        sale_emoji = get_sale_emoji(sale_percentage, uah_sale)
        message = (f"{sale_emoji}  New item  {sale_emoji}\n{shoe['name']}\n\n"
                   f"💀 Prices : <s>{shoe['original_price']}</s>  <b>{shoe['sale_price']}</b>  <i>(Sale: <b>{sale_percentage}%</b>)</i>\n"
                   f"🤑 Grivniki : <b>{uah_sale} UAH </b>\n"
                   f"🧠 UAH Diff : <b>{uah_orig}</b>\n"
                   f"🧊 Kurs : {kurs_symbol} {kurs} \n"
                   f"🔗 Store : <a href='{shoe['shoe_link']}'>{shoe['store']}</a>\n"
                   f"🌍 Country : {country}")
        message_id = await message_queue.add_message(shoe['base_url']['telegram_chat_id'], message, shoe['image_url'], uah_sale, sale_percentage)
        
        # Wait for the message to be sent successfully before marking as processed
        while not message_queue.is_message_sent(message_id):
            await asyncio.sleep(1)
            
        processed_shoes.add(key)  # Only mark as processed after successful send
        old_data[key] = shoe
        save_shoe_data(old_data)
    elif old_data[key]['sale_price'] != shoe['sale_price'] or not old_data[key].get('active', True):
        old_sale_price = old_data[key]['sale_price']
        old_sale_country = old_data[key]['country']
        if 'uah_price' not in old_data[key]:
            old_price_data = convert_to_uah(old_sale_price, old_sale_country, exchange_rates, shoe['name'])
            old_uah_sale = old_price_data.uah_amount
        else:
            old_uah_sale = old_data[key]['uah_price']
        shoe['uah_price'] = uah_sale        
        old_price_data = convert_to_uah(shoe['sale_price'], shoe['country'], exchange_rates, shoe['name'])
        old_price = old_price_data.uah_amount
        price_difference = old_price - uah_sale
        
        lowest_price_uah = old_data[key].get('lowest_price_uah', old_uah_sale)
        if uah_sale < lowest_price_uah:
            shoe['lowest_price'] = shoe['sale_price']
            shoe['lowest_price_uah'] = uah_sale
        else:
            shoe['lowest_price'] = old_data[key].get('lowest_price', old_sale_price)
            shoe['lowest_price_uah'] = lowest_price_uah

        shoe['active'] = True
        if price_difference >= 400 or (not old_data[key].get('active', True) and price_difference >= 400):
            status = "Update" if old_data[key].get('active', True) else "Back in stock"
            message = (f"💎💎💎 {status} 💎💎💎 \n{shoe['name']}:\n\n"
                       f"💀 Prices : <s>{shoe['original_price']}</s>  <s>{old_sale_price}</s>  <b>{shoe['sale_price']}</b>  <i>(Sale: <b>{sale_percentage}%</b>)</i> \n"
                       f"🤑 Grivniki : {uah_sale} UAH\n"
                       f"📉 Lowest price : {shoe['lowest_price']} ({shoe['lowest_price_uah']} UAH)\n"
                       f"🧊 Kurs : {kurs_symbol} {kurs} \n"
                       f"🔗 Store : <a href='{shoe['shoe_link']}'>{shoe['store']}</a>\n"
                       f"🌍 Country : {country}")
            message_id = await message_queue.add_message(shoe['base_url']['telegram_chat_id'], message, shoe['image_url'], uah_sale, sale_percentage)
            
            while not message_queue.is_message_sent(message_id):
                await asyncio.sleep(1)
                
            processed_shoes.add(key)

        shoe['active'] = True
        old_data[key] = shoe
        save_shoe_data(old_data)

async def process_country(base_url, country):
    logger.info(f"Starting scrape for {country} | {base_url['url_name']}")
    try:
        current_shoes = await scrape_all_pages(base_url, country)
        return current_shoes
    except Exception as e:
        logger.error(f"Error during scrape for {country} | {base_url['url_name']}: {e}")
        logger.error(traceback.format_exc())
        return []
    
async def process_all_shoes(all_shoes, old_data, message_queue, exchange_rates):
    new_shoe_count = 0
    semaphore = asyncio.Semaphore(25)
    total_items = len(all_shoes)

    async def process_single_shoe(i, shoe):
        nonlocal new_shoe_count
        try:
            country = shoe['country']
            name = shoe['name']
            unique_id = shoe['unique_id']
            store = shoe['store']
            key = f"{name}_{unique_id}"
            sale_percentage = calculate_sale_percentage(shoe['original_price'], shoe['sale_price'], country)
            
            if sale_percentage < shoe['base_url']['min_sale']:
                return

            if key not in old_data:
                shoe['shoe_link'] = await get_final_clear_link(shoe['shoe_link'], semaphore, name, country, i, total_items, store)
                new_shoe_count += 1
            else:
                shoe['shoe_link'] = old_data[key]['shoe_link']
            
            await process_shoe(shoe, old_data, message_queue, exchange_rates)
        except Exception as e:
            logger.error(f"Error processing shoe {name} for {country} | {shoe['base_url']['url_name']}: {e}")
            logger.error(traceback.format_exc())

    # Create tasks for all shoes
    tasks = [process_single_shoe(i, shoe) for i, shoe in enumerate(all_shoes, 1)]

    # Run all tasks concurrently
    await asyncio.gather(*tasks)

    logger.info(f"Processed {new_shoe_count} new shoes in total")

    # Handle removed shoes
    current_shoes = {f"{shoe['name']}_{shoe['unique_id']}" for shoe in all_shoes}
    for key in list(old_data.keys()):
        if key not in current_shoes:
            if old_data[key].get('active', True):
                old_data[key]['active'] = False
                save_shoe_data(old_data)

async def process_url(base_url, countries, exchange_rates):
    all_shoes = []
    
    async def scrape_country(country):
        page = 1
        country_shoes = []
        while True:
            url = f"{base_url['url']}&page={page}"
            logger.info(f"Scraping page {page} for country {country} - {base_url['url_name']}")
            shoes = await scrape_page(url, country)
            if not shoes:
                logger.info(f"Stopping at page {page} - {country} - {base_url['url_name']}.")
                break
            country_shoes.extend(shoes)
            page += 1
            await asyncio.sleep(1)
        return country_shoes

    country_tasks = [scrape_country(country) for country in countries]
    country_results = await asyncio.gather(*country_tasks)
    
    for country, result in zip(countries, country_results):
        for shoe in result:
            if isinstance(shoe, dict):
                shoe['base_url'] = base_url
                all_shoes.append(shoe)
            else:
                logger.error(f"Unexpected item data type for {country}: {type(shoe)}")
        special_logger.info(f"Found {len(result)} items for {country} - {base_url['url_name']}")

    return all_shoes

async def run_country_process(base_url, country, old_data, bot_token, exchange_rates):
    try:
        return await process_country(base_url, country, old_data, bot_token, exchange_rates)
    except Exception as e:
        logger.error(f"Error processing country {country}: {e}")
        logger.error(traceback.format_exc())
        return None

def country_worker(base_url, country, old_data, bot_token, exchange_rates):
    try:
        asyncio.run(run_country_process(base_url, country, old_data, bot_token, exchange_rates))
    except Exception as e:
        logger.error(f"Error in country worker for {country}: {e}")
        logger.error(traceback.format_exc())

def url_worker(base_url, countries, old_data, bot_token, exchange_rates):
    try:
        return asyncio.run(process_url(base_url, countries, old_data, bot_token, exchange_rates))
    except Exception as e:
        logger.error(f"Error in URL worker for {base_url['url_name']}: {e}")
        logger.error(traceback.format_exc())


def print_statistics():
    special_logger.stat(f"Max wait time for initial URL change: {max_wait_times['url_changes']:.2f} seconds")
    special_logger.stat(f"Max wait time for final URL change: {max_wait_times['final_url_changes']:.2f} seconds")
    
    special_logger.stat("Store Statistics:")
    for store, stats in store_statistics.items():
        success_rate = (stats['success'] / stats['total']) * 100 if stats['total'] > 0 else 0
        special_logger.stat(f"{store}: {success_rate:.2f}% success rate ({stats['success']}/{stats['total']})")
        
def print_link_statistics():
    special_logger.stat("Link Processing Statistics:")
    for step, stats in link_statistics.items():
        if step != 'other_failures':
            success_rate = (stats['success'] / (stats['success'] + stats['fail'])) * 100 if (stats['success'] + stats['fail']) > 0 else 0
            special_logger.stat(f"{step}: {success_rate:.2f}% success rate ({stats['success']}/{stats['success'] + stats['fail']})")
            if stats['fail'] > 0:
                special_logger.stat(f"  Failed links for {step} (showing up to 10):")
                for link in stats['fail_links'][:10]:
                    special_logger.stat(f"    {link}")
        else:
            special_logger.stat(f"Other failures: {stats['count']}")
            if stats['count'] > 0:
                special_logger.stat(f"  Failed links for other failures (showing up to 10):")
                for link in stats['links'][:10]:
                    special_logger.stat(f"    {link}")
                    
def center_text(text, width, fill_char=' '):
    padding = (width - len(text)) // 2
    return fill_char * padding + text + fill_char * (width - padding - len(text))

async def main():
    global processed_shoes, LIVE_MODE
    message_queue = TelegramMessageQueue(TELEGRAM_BOT_TOKEN)
    asyncio.create_task(message_queue.process_queue())
    
    # Initialize the bot with command handlers
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    application.add_handler(CommandHandler("ver", ver_command))
    application.add_handler(CommandHandler("linkstat", linkstat_command))

    # Start the bot
    await application.initialize()
    await application.start()
    await application.updater.start_polling()

    # Get terminal width
    terminal_width = shutil.get_terminal_size().columns
    
    # Print green line
    print(Fore.GREEN + '-' * terminal_width + Style.RESET_ALL)
    
    # Print centered bot version with no timestamp
    bot_version = f"Grotesk bot v.{BOT_VERSION}"
    centered_version = center_text(bot_version, terminal_width)
    print(Fore.CYAN + Style.BRIGHT + centered_version + Style.RESET_ALL)
    
    # Print green line
    print(Fore.GREEN + '-' * terminal_width + Style.RESET_ALL)

    if ASK_FOR_LIVE_MODE:
        live_mode_input = input("Enter 'live' to enable live mode, or press Enter to continue in headless mode: ").strip().lower()
        LIVE_MODE = (live_mode_input == 'live')

    if LIVE_MODE:
        special_logger.good("Live mode enabled")

    try:
        while True:
            try:
                processed_shoes = set()  # Reset at the start of each run
                old_data = load_shoe_data()
                exchange_rates = load_exchange_rates()

                all_shoes = []
                url_tasks = [process_url(base_url, COUNTRIES, exchange_rates) for base_url in BASE_URLS]
                url_results = await asyncio.gather(*url_tasks)
                
                for result in url_results:
                    all_shoes.extend(result)
                unfiltered_len = len(all_shoes)
                all_shoes = filter_duplicates(all_shoes, exchange_rates)
                special_logger.stat(f"Removed {unfiltered_len - len(all_shoes)} duplicates")
                
                await process_all_shoes(all_shoes, old_data, message_queue, exchange_rates)

                # Print statistics
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
        # Stop the application when the main loop exits
        await application.stop()

if __name__ == "__main__":
    asyncio.run(main())
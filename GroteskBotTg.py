import json
import time
import asyncio
import logging
import colorlog
import traceback
import urllib.parse
import re
from collections import defaultdict, namedtuple
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from telegram import Bot
from telegram.error import RetryAfter, TimedOut
from config import TELEGRAM_BOT_TOKEN, EXCHANGERATE_API_KEY, BASE_URLS

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

def setup_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    formatter = colorlog.ColoredFormatter(
        '%(log_color)s%(asctime)s - %(message)s',
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'white',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'red,bg_white',
            'STAT': 'magenta',
            'GOOD': 'green',
            'LIGHTBLUE_INFO': 'light_blue',
        },
        datefmt='%d.%m %H:%M:%S'
    )

    handler = CompactGroupedMessageHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger

class TelegramMessageQueue:
    def __init__(self, bot_token):
        self.queue = asyncio.Queue()
        self.bot_token = bot_token

    async def add_message(self, chat_id, message, image_url=None):
        await self.queue.put(TelegramMessage(chat_id, message, image_url))

    async def process_queue(self):
        while True:
            message = await self.queue.get()
            success = await send_telegram_message(self.bot_token, message.chat_id, message.message, message.image_url)
            if not success:
                # If sending fails, put the message back in the queue
                await self.queue.put(message)
            await asyncio.sleep(1)  # Delay to prevent hitting rate limi

logger = setup_logger()

class SpecialLogger:
    @staticmethod
    def stat(message):
        logger.log(35, f"\033[95m{message}\033[0m")  # 35 is a custom log level

    @staticmethod
    def good(message):
        logger.log(25, f"\033[92m{message}\033[0m")  # 25 is another custom log level between INFO and WARNING
        
    @staticmethod
    def info(message):
        logger.log(22, f"\033[94m{message}\033[0m") # 22 is a new custom log level for light blue

processed_shoes = set()
TelegramMessage = namedtuple('TelegramMessage', ['chat_id', 'message', 'image_url'])

# Add these custom log levels
logging.addLevelName(35, "STAT")
logging.addLevelName(25, "GOOD")
logging.addLevelName(22, "LIGHTBLUE_INFO")


special_logger = SpecialLogger()

LIVE_MODE = False  # Set to True to enable live mode by default
ASK_FOR_LIVE_MODE = False  # Set to False to skip asking for live mode at startup

# Files to store data
SHOE_DATA_FILE = 'shoe_data.json'
EXCHANGE_RATES_FILE = 'exchange_rates.json'

# Countries to scrape
COUNTRIES = ['IT', 'PL', 'US', 'GB']

max_wait_times = {'url_changes': 0, 'final_url_changes': 0}
store_statistics = defaultdict(lambda: {'success': 0, 'total': 0})
link_statistics = {
    'tradedoubler_linksynergy_prf': {'success': 0, 'fail': 0, 'fail_links': []},
    'lyst_track_lead': {'success': 0, 'fail': 0, 'fail_links': []},
    'click_here': {'success': 0, 'fail': 0, 'fail_links': []},
    'other_failures': {'count': 0, 'links': []}
}


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
                            link_statistics['tradedoubler_linksynergy_prf']['success'] += 1
                
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
            logger.warning("Giuseppe Zanotti blyat")
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

        # Skip items with original price less than 100
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
        else:
            logger.info(f"Failed or skipped item in {country}")
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
        await asyncio.sleep(1)  # Add a delay between requests
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

async def send_telegram_message(bot_token, chat_id, message, image_url=None, max_retries=3):
    bot = Bot(token=bot_token)
    for attempt in range(max_retries):
        try:
            if image_url and image_url.startswith(('http://', 'https://')):
                result = await bot.send_photo(chat_id=chat_id, photo=image_url, caption=message, parse_mode='HTML')
            else:
                result = await bot.send_message(chat_id=chat_id, text=message, parse_mode='HTML')
            
            logger.info(f"Message sent successfully on attempt {attempt + 1}")
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
                shoe['uah_price'] = convert_to_uah(shoe['sale_price'], shoe['country'], exchange_rates, shoe['name'])

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
            amount = float(price.replace('€', '').replace(',', '.').strip())
        elif '£' in price:
            currency = 'GBP'
            amount = float(price.replace('£', '').replace(',', '').strip())
        elif '$' in price:
            currency = 'USD'
            amount = float(price.replace('$', '').replace(',', '').strip())
        else:
            logger.error(f"Unrecognized currency symbol in price '{price}' for '{name}' country '{country}'")
            return 0

        if currency in exchange_rates:
            rate = exchange_rates[currency]
        else:
            logger.error(f"Exchange rate not found for currency '{currency}' (country: {country})")
            return 0
        
        uah_amount = amount / rate
        
        return round(uah_amount, 2)
    
    except ValueError as e:
        logger.error(f"Error converting price '{price}' for '{name}' country '{country}': {e}")
        return 0
    except KeyError as e:
        logger.error(f"Missing exchange rate for currency '{currency}': {e}")
        return 0

def get_sale_emoji(sale_percentage, uah_sale):
    if sale_percentage >= 75 :
        return "💊💊💊 " 
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
    uah_original = convert_to_uah(shoe['original_price'], country, exchange_rates, shoe['name'])
    uah_sale = convert_to_uah(shoe['sale_price'], country, exchange_rates, shoe['name'])

    if key not in old_data:
        # New item
        shoe['lowest_price'] = shoe['sale_price']
        shoe['lowest_price_uah'] = uah_sale
        shoe['active'] = True
        sale_emoji = get_sale_emoji(sale_percentage, uah_sale)
        message = (f"{sale_emoji}  New item  {sale_emoji}\n{shoe['name']}\n\n"
                   f"💀 Original price: {shoe['original_price']} ({uah_original} UAH)\n"
                   f"💰 Sale price: {shoe['sale_price']} ({uah_sale} UAH)\n"
                   f"🤑 Sale percentage: {sale_percentage}%\n"
                   f"🔗 Store: <a href='{shoe['shoe_link']}'>{shoe['store']}</a>\n"
                   f"🌍 Country : {country}")
        await message_queue.add_message(shoe['base_url']['telegram_chat_id'], message, shoe['image_url'])
        old_data[key] = shoe
        save_shoe_data(old_data)
        processed_shoes.add(key)  # Mark as processed
    elif old_data[key]['sale_price'] != shoe['sale_price'] or not old_data[key].get('active', True):
        old_sale_price = old_data[key]['sale_price']
        old_sale_percentage = calculate_sale_percentage(old_data[key]['original_price'], old_sale_price, country)
        old_uah_sale = convert_to_uah(old_sale_price, country, exchange_rates, shoe['name'])
        
        old_price = convert_to_uah(old_sale_price, country, exchange_rates, shoe['name'])
        new_price = convert_to_uah(shoe['sale_price'], country, exchange_rates, shoe['name'])
        price_difference = old_price - new_price
        
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
                       f"💀 Original price: {shoe['original_price']} ({uah_original} UAH)\n"
                       f"👨‍🦳 Old price: {old_sale_price} ({old_uah_sale} UAH) (Sale: {old_sale_percentage}%)\n"
                       f"👶 New price: {shoe['sale_price']} ({uah_sale} UAH) (Sale: {sale_percentage}%)\n"
                       f"📉 Lowest price: {shoe['lowest_price']} ({shoe['lowest_price_uah']} UAH)\n"
                       f"🔗 Store: <a href='{shoe['shoe_link']}'>{shoe['store']}</a>\n"
                       f"🌍 Country: {country}")
            await message_queue.add_message(shoe['base_url']['telegram_chat_id'], message, shoe['image_url'])
            processed_shoes.add(key)  # Mark as processed

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
            if isinstance(shoe, dict):  # Ensure shoe is a dictionary before modifying
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

async def main():
    global processed_shoes, LIVE_MODE
    message_queue = TelegramMessageQueue(TELEGRAM_BOT_TOKEN)
    asyncio.create_task(message_queue.process_queue())

    if ASK_FOR_LIVE_MODE:
        live_mode_input = input("Enter 'live' to enable live mode, or press Enter to continue in headless mode: ").strip().lower()
        LIVE_MODE = (live_mode_input == 'live')

    if LIVE_MODE:
        special_logger.good("Live mode enabled")
    else:
        special_logger.good("Running in headless mode.")

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
            special_logger.stat(f"Were removed {unfiltered_len - len(all_shoes)} duplicates")
            
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
            logger.info("Waiting for 60 seconds before retrying")
            await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(main())
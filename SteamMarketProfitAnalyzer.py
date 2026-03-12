import asyncio
import re
import math
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
import signal

class ImprovedSteamMarketAnalyzer:
    def __init__(self):
        self.browser = self.context = self.page = self.playwright = None
    
    async def initialize(self):
        """Initialize the Playwright browser instance with Ukrainian currency settings."""
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.firefox.launch(headless=True)
        self.context = await self.browser.new_context()
        await self.context.add_cookies([
            {"name": "steamCountry", "value": "UA%7C%7C7981e29e7c1ade9e577e45cdef", "domain": ".steamcommunity.com", "path": "/"},
            {"name": "steamLoginSecure", "value": "76561198075576947%7C%7CeyAidHlwIjogIkpXVCIsICJhbGciOiAiRWREU0EiIH0.eyAiaXNzIjogInI6MDAwNl8yNjRENjk5RF9FNkM5RCIsICJzdWIiOiAiNzY1NjExOTgwNzU1NzY5NDciLCAiYXVkIjogWyAid2ViOmNvbW11bml0eSIgXSwgImV4cCI6IDE3NDg3MTU2MzEsICJuYmYiOiAxNzM5OTg4MDg3LCAiaWF0IjogMTc0ODYyODA4NywgImp0aSI6ICIwMDE0XzI2NUZFMTU5XzIzNjQyIiwgIm9hdCI6IDE3NDcyNTIxNTEsICJydF9leHAiOiAxNzY1NDA3MTY1LCAicGVyIjogMCwgImlwX3N1YmplY3QiOiAiMTc2LjM4LjIyLjE1NCIsICJpcF9jb25maXJtZXIiOiAiMTc2LjM4LjIyLjE1NCIgfQ.xb1nSRroxtLgJwxJi1jSV9T_gNKLxsMQ5kNr_3R8whKRuZJIxF3Qksb0L_AjOR_x26RM3gjnBz512QKNR8pHBA", "domain": ".steamcommunity.com", "path": "/"},
            {"name": "strLanguage", "value": "english", "domain": ".steamcommunity.com", "path": "/"}
        ])
        self.page = await self.context.new_page()
        await self.page.goto("https://steamcommunity.com/")
        await self.page.wait_for_load_state("networkidle")
    
    async def close(self):
        """Close browser and clean up resources."""
        for resource in [self.page, self.context, self.browser, self.playwright]:
            if resource:
                await resource.close() if resource != self.playwright else await resource.stop()
        self.page = self.context = self.browser = self.playwright = None
    
    async def search_for_item_variants(self, item_name):
        """Search for item variants on Steam Market."""
        await self.page.goto(f"https://steamcommunity.com/market/search?q={item_name.replace(' ', '+')}")
        await self.page.wait_for_load_state("networkidle")
        try:
            await self.page.wait_for_selector(".market_listing_row_link", timeout=10000, state="visible")
        except Exception as e:
            print(f"Warning: No search results found for '{item_name}': {e}")
            return []
        
        variant_links = await self.page.evaluate('''() => {
            const links = document.querySelectorAll(".market_listing_row_link");
            return Array.from(links).map(link => ({
                url: link.href,
                name: link.querySelector(".market_listing_item_name")?.innerText || "Unknown Item"
            }));
        }''')
        print(f"Found {len(variant_links)} variants for '{item_name}'")
        return variant_links
    
    async def extract_item_name_and_game(self, url):
        """Extract item name and game from the URL."""
        await self.page.goto(url)
        await self.page.wait_for_load_state("networkidle")
        item_name = await self.page.title()
        item_name = item_name.split(" :: ")[1] if " :: " in item_name else item_name
        game_element = await self.page.query_selector(".market_listing_game_name")
        game_name = await game_element.text_content() if game_element else "Unknown Game"
        return item_name, game_name
    
    def parse_steam_date(self, date_str):
        """Parse Steam's date format which can be complex and include time components."""
        date_str = date_str.strip()
        # Handle partial time component
        if re.search(r'\d{2}:', date_str) and not re.search(r'\d{2}:\d{2}', date_str):
            date_str = re.sub(r'(\d{2}):', r'\1:00', date_str)
        
        # Try different known Steam date formats
        formats = ["%b %d %Y", "%b %d %Y %H:%M:%S", "%b %d %Y %H:%M", "%d %b %Y", 
                   "%d %b %Y %H:%M:%S", "%Y-%m-%d", "%Y-%m-%d %H:%M:%S"]
        
        # Try all formats, then try extracting just the date part if all fail
        for fmt in formats:
            try: return datetime.strptime(date_str, fmt)
            except ValueError: continue
        
        match = re.search(r'([A-Za-z]{3} \d{1,2} \d{4}|\d{1,2} [A-Za-z]{3} \d{4}|\d{4}-\d{2}-\d{2})', date_str)
        if match:
            for fmt in ["%b %d %Y", "%d %b %Y", "%Y-%m-%d"]:
                try: return datetime.strptime(match.group(1), fmt)
                except ValueError: continue
        return None
    
    async def get_current_listings(self, url):
        """Get current listings to analyze current market prices."""
        await self.page.goto(url)
        await self.page.wait_for_load_state("networkidle")
        show_more_button = await self.page.query_selector('#market_buyorder_info_show_details')
        if show_more_button:
            await show_more_button.click()
            await self.page.wait_for_timeout(1000)
        
        listing_data = await self.page.evaluate('''() => {
            const listings = document.querySelectorAll('.market_listing_price.market_listing_price_with_fee');
            return Array.from(listings).map(item => ({
                raw: item.innerText.trim(),
                price: item.innerText.trim().replace(/[^0-9.,]/g, '').replace(',', '.')
            }));
        }''')
        
        prices = []
        for item in listing_data:
            try:
                if "₴" in item["raw"] or "UAH" in item["raw"] or True:  # Accept all prices but warn if not UAH
                    if "₴" not in item["raw"] and "UAH" not in item["raw"]:
                        print(f"Warning: Price might not be in UAH: {item['raw']}")
                    prices.append(float(item["price"]))
            except ValueError: pass
        return prices
    
    async def get_buy_requests(self, url):
        """Extract the number of buy requests for an item."""
        await self.page.goto(url)
        await self.page.wait_for_load_state("networkidle")
        show_more_button = await self.page.query_selector('#market_buyorder_info_show_details')
        if show_more_button:
            await show_more_button.click()
            await self.page.wait_for_timeout(1000)
        
        return await self.page.evaluate('''() => {
            // Try different selectors to find buy requests count
            const selectors = [
                {
                    container: '#market_commodity_buyrequests',
                    target: '.market_commodity_orders_header_promote',
                    pattern: /([0-9,]+)/
                },
                {
                    container: '#market_buyorder_info',
                    pattern: /([0-9,]+)\\s+buy\\s+orders/i
                },
                {
                    container: '.market_commodity_orders_header_promote',
                    pattern: /([0-9,]+)/
                }
            ];
            
            for (const selector of selectors) {
                const container = document.querySelector(selector.container);
                if (!container) continue;
                
                const content = selector.target ? 
                    container.querySelector(selector.target)?.textContent : 
                    container.textContent;
                
                if (content) {
                    const match = content.match(selector.pattern);
                    if (match && match[1]) return parseInt(match[1].replace(/,/g, ''));
                }
            }
            
            return 0; // Default if we can't find any buy orders
        }''')
    
    async def get_price_history(self, url):
        """Extract price history data from the Steam item page."""
        await self.page.goto(url)
        await self.page.wait_for_load_state("networkidle")
        
        currency_indicator = await self.page.evaluate('''() => {
            const priceElements = document.querySelectorAll('.market_listing_price_with_fee');
            return priceElements.length > 0 ? priceElements[0].innerText.trim() : null;
        }''')
        
        if currency_indicator and "₴" not in currency_indicator and "UAH" not in currency_indicator:
            print(f"Warning: Currency might not be UAH. Sample price: {currency_indicator}")
        
        try:
            await self.page.wait_for_selector("#pricehistory", timeout=10000)
        except Exception as e:
            print(f"Warning: Price history not available: {e}")
            return None
        
        return await self.page.evaluate('''() => {
            if (typeof g_rgPriceHistory !== 'undefined') return g_rgPriceHistory;
            
            const scripts = document.querySelectorAll('script');
            for (const script of scripts) {
                const text = script.textContent;
                if (text && text.includes('var line1=')) {
                    const match = text.match(/var line1=([^;]+);/);
                    if (match && match[1]) {
                        try { return JSON.parse(match[1]); } 
                        catch (e) { console.log("Failed to parse price history:", e); }
                    }
                }
            }
            return null;
        }''')
    
    async def analyze_item(self, url):
        """Analyze a single item's price and profit potential using percentile-based pricing."""
        item_name, game_name = "Unknown Item", "Unknown Game"
        try:
            item_name, game_name = await self.extract_item_name_and_game(url)
            current_listings = await self.get_current_listings(url)
            price_history = await self.get_price_history(url)
            buy_requests_count = await self.get_buy_requests(url)
        
            # No data available
            if not price_history or not current_listings:
                return {"item_name": item_name, "game": game_name, "url": url,
                       "error": "Insufficient data available for analysis", "has_data": False}
        
            # Find current lowest price
            current_lowest = min(current_listings) if current_listings else None
        
            # Create a weighted price list that accounts for volume
            weighted_prices, price_volume_map = [], {}
            two_weeks_ago = datetime.now() - timedelta(days=14)
        
            if price_history:
                for entry in price_history:
                    if len(entry) >= 3:  # Price history entry with volume
                        try:
                            date = self.parse_steam_date(entry[0])
                            if date is None: continue
                            price, volume = float(entry[1]), int(float(entry[2]))
                            
                            # Only consider sales from last 2 weeks
                            if date >= two_weeks_ago:
                                # Add price to weighted list based on volume
                                weighted_prices.extend([price] * volume)
                                
                                # Store price with its volume for reference
                                price_key = round(price, 2)  # Group similar prices
                                if price_key not in price_volume_map:
                                    price_volume_map[price_key] = {'volume': 0, 'dates': []}
                                
                                price_volume_map[price_key]['volume'] += volume
                                price_volume_map[price_key]['dates'].append(date)
                        except (ValueError, TypeError, IndexError): continue
        
            # Cannot analyze without pricing data
            if not weighted_prices:
                return {"item_name": item_name, "game": game_name, "url": url,
                       "error": "Insufficient historical pricing data", "has_data": False}
        
            # Sort weighted prices for percentile calculations
            weighted_prices.sort()
            total_sales = len(weighted_prices)
            
            # Calculate buy/sell prices
            if total_sales < 3:
                buy_price, sell_price = min(weighted_prices), max(weighted_prices)
            else:
                buy_index = max(2, int(total_sales * 0.01) - 1)  # 1st percentile
                sell_index = min(total_sales - 3, int(total_sales * 0.99))  # 99th percentile
                            
                # Adjust indices to ensure at least 3 distinct sales
                if sell_index - buy_index < 2:
                    if sell_index + 2 < total_sales: 
                        sell_index += 2
                    elif buy_index - 2 >= 0: 
                        buy_index -= 2
                    else: 
                        buy_price, sell_price = min(weighted_prices), max(weighted_prices)
                else:
                    buy_price, sell_price = weighted_prices[buy_index], weighted_prices[sell_index]
        
            # Ensure buy price is below sell price with some margin
            if buy_price >= sell_price:
                buy_price, sell_price = min(weighted_prices), max(weighted_prices)
                # If still problematic, use current price as reference
                if buy_price >= sell_price and current_lowest:
                    buy_price, sell_price = current_lowest * 0.9, current_lowest * 1.1
        
            # Calculate price metrics
            suggested_buying_price = round(buy_price + 0.01, 2)  # Slightly above 1st percentile
            potential_selling_price = round(sell_price - 0.01, 2)  # Slightly below 99th percentile
        
            # Calculate profit metrics
            net_revenue = potential_selling_price * 0.85  # After Steam's 15% fee
            potential_profit = net_revenue - suggested_buying_price
            profit_percentage = (potential_profit / suggested_buying_price) * 100 if suggested_buying_price > 0 else 0
        
            current_net_revenue = current_lowest * 0.85 if current_lowest else 0
            current_price_profit = current_net_revenue - suggested_buying_price if current_lowest else 0
            current_profit_percentage = (current_price_profit / suggested_buying_price) * 100 if suggested_buying_price > 0 else 0
        
            # Calculate statistical metrics
            if weighted_prices:
                mean = sum(weighted_prices) / len(weighted_prices)
                median_price = weighted_prices[len(weighted_prices) // 2]
                price_range = max(weighted_prices) - min(weighted_prices)
                volatility_percentage = (price_range / mean) * 100 if mean > 0 else 0
                
                # Calculate standard deviation for volatility
                variance = sum((x - mean) ** 2 for x in weighted_prices) / len(weighted_prices)
                volatility = variance ** 0.5
            else:
                mean = median_price = price_range = volatility_percentage = volatility = None
        
            # Calculate risk-adjusted metrics
            M1_ratio = M2_ratio = None
            if volatility and volatility > 0:
                M1 = ((0.85 * potential_selling_price) - suggested_buying_price) / suggested_buying_price
                M2 = ((0.85 * current_lowest) - suggested_buying_price) / suggested_buying_price if current_lowest else 0
                M1_ratio, M2_ratio = M1 / volatility, M2 / volatility
        
            raps = calculate_raps(M1_ratio, M2_ratio)
            my_testing_raps = current_price_profit*0.66 + potential_profit * 0.34 + ((current_price_profit*0.66 + potential_profit * 0.34) / suggested_buying_price)
            
            # Calculate total monthly volume
            total_monthly_volume = sum(v['volume'] for v in price_volume_map.values())
            
            # Calculate enhanced RAPS score based on volume, price potential, and buy requests
            currently_working_raps = 100 * (
                ((0.85 * current_lowest - suggested_buying_price) + 0.85 * (0.85* potential_selling_price - suggested_buying_price)) / 
                (suggested_buying_price + 0.01)
            ) * ((total_monthly_volume + 1) / ((buy_requests_count) + 1)) * math.sqrt(total_monthly_volume + 1) / math.sqrt(suggested_buying_price + 0.01)
        
            # Return result
            return {
                "item_name": item_name, "game": game_name, "url": url,
                "current_lowest_price": current_lowest,
                "suggested_buying_price": suggested_buying_price,
                "potential_selling_price": potential_selling_price,
                "buy_percentile": "1st percentile", "sell_percentile": "99th percentile",
                "net_revenue_after_fees": net_revenue,
                "potential_profit": potential_profit, "profit_percentage": profit_percentage,
                "current_price_profit": current_price_profit, "current_profit_percentage": current_profit_percentage,
                "price_volatility": volatility_percentage, "volatility": volatility,
                "average_price": mean, "median_price": median_price,
                "total_sales_analyzed": total_sales,
                "buy_requests_count": buy_requests_count,
                "analysis_date": datetime.now().strftime("%Y-%m-%d %H:%M"),
                "currency": "UAH",
                "price_volume_data": {str(k): v['volume'] for k, v in price_volume_map.items()},
                "total_monthly_volume": total_monthly_volume,
                "has_data": True, "M1_ratio": M1_ratio, "M2_ratio": M2_ratio,
                "raps_score": currently_working_raps, "my_raps": my_testing_raps,
            }
        except Exception as e:
            print(f"Error analyzing {url}: {e}")
            return {"item_name": item_name, "game": game_name, "url": url, "error": str(e), "has_data": False}

def calculate_raps(m1_ratio, m2_ratio, w1=0.7, w2=0.3, penalty_factor=0.5):
    """Calculate Risk-Adjusted Profit Score (RAPS)."""
    if m1_ratio is None or m2_ratio is None: return None
    m2_component = m2_ratio * (penalty_factor if m2_ratio < 0 else 1.0)
    return (w1 * m1_ratio) + (w2 * m2_component)

class ParallelSteamAnalyzer:
    """Class for parallel analysis of multiple Steam market items."""
    
    def __init__(self, max_workers=5):
        """Initialize with the maximum number of parallel workers (5 by default)."""
        self.max_workers = max_workers
        self.semaphore = asyncio.Semaphore(max_workers)  # Control concurrent browser instances
        self.analyzers = []  # Keep track of all created analyzers for proper cleanup
    
    async def create_analyzer_instance(self):
        """Create and initialize a new analyzer instance."""
        async with self.semaphore:  # Limit concurrent browser instances
            analyzer = ImprovedSteamMarketAnalyzer()
            await analyzer.initialize()
            self.analyzers.append(analyzer)
            return analyzer
    
    async def cleanup_all_analyzers(self):
        """Clean up all analyzer instances."""
        if self.analyzers:
            await asyncio.gather(*[analyzer.close() for analyzer in self.analyzers], return_exceptions=True)
            self.analyzers = []
    
    async def analyze_batch(self, urls, batch_id):
        """Analyze a batch of URLs using a single analyzer instance."""
        analyzer = await self.create_analyzer_instance()
        try:
            results = []
            for i, url in enumerate(urls):
                print(f"Processing batch {batch_id}, item {i+1}/{len(urls)}: {url}")
                try:
                    results.append(await analyzer.analyze_item(url))
                except Exception as e:
                    print(f"Error analyzing {url}: {e}")
                    results.append({"url": url, "error": str(e), "has_data": False})
                await asyncio.sleep(0.5)  # Small delay between requests
            return results
        except Exception as e:
            print(f"Error in batch {batch_id}: {e}")
            return []
    
    async def analyze_all_variants_parallel(self, item_name):
        """Find all variants of an item and analyze them in parallel."""
        initial_analyzer = ImprovedSteamMarketAnalyzer()
        try:
            await initial_analyzer.initialize()
            self.analyzers.append(initial_analyzer)  # Add to tracked analyzers
            variants = await initial_analyzer.search_for_item_variants(item_name)
            
            if not variants:
                print(f"No variants found for '{item_name}'")
                return {"error": f"No variants found for '{item_name}'", "variants": []}
                
            # Get all URLs to process
            urls = [variant['url'] for variant in variants]
            total_urls = len(urls)
            
            # Use dynamic batch sizing based on number of variants
            batch_size = max(1, min(5, total_urls // self.max_workers))
            batches = [urls[i:i+batch_size] for i in range(0, total_urls, batch_size)]
            
            print(f"Processing {total_urls} variants in {len(batches)} batches with {self.max_workers} workers")
            
            # Process batches in parallel 
            batch_results = await asyncio.gather(*[self.analyze_batch(batch, i) for i, batch in enumerate(batches)])
            
            # Flatten results
            all_results = [item for sublist in batch_results for item in sublist]
            
            # Filter out variants with insufficient data
            valid_results = [r for r in all_results if r.get('has_data', False)]
            
            if not valid_results:
                return {"error": "None of the variants had sufficient data for analysis", "variants": all_results}
            
            # Find the best variant based on RAPS score
            best_variant = max(valid_results, key=lambda x: x.get('raps_score', float('-inf')) if x.get('raps_score') is not None else float('-inf'))
            
            return {
                "best_variant": best_variant, 
                "all_variants": all_results,
                "total_variants_analyzed": len(all_results), 
                "valid_variants": len(valid_results)
            }
        finally:
            # Ensure cleanup happens
            await self.cleanup_all_analyzers()

async def analyze_steam_item_by_name(item_name):
    """Analyze a Steam item by name with optimized parallel processing."""
    analyzer = ParallelSteamAnalyzer(max_workers=5)  # Always use 5 workers as requested
    try:
        result = await analyzer.analyze_all_variants_parallel(item_name)
        
        # Print formatted results
        print("\n=== Steam Market Analysis Results ===")
        all_variants = result["all_variants"]
        
        print("\n=== ALL ANALYZED VARIANTS (Sorted by RAPS Score) ===")
        # Sort by RAPS score
        sorted_variants = sorted(
            [v for v in all_variants if v.get('has_data', False) and v.get('raps_score') is not None],
            key=lambda x: x['raps_score'],
            reverse=True
        )
        
        for i, variant in enumerate(sorted_variants[:10], 1):
            print(f"{i}. {variant['item_name']}")
            print(f"   Buy at: ₴{variant['suggested_buying_price']:.2f}")
            print(f"   Sell at: ₴{variant['potential_selling_price']:.2f}")
            print(f"   Current price: ₴{variant['current_lowest_price']:.2f}")
            print(f"   Volume to buy: {round((variant.get('total_monthly_volume', 0) / max(variant.get('buy_requests_count', 1), 1) * 100), 2)}")
            print(f"   Profit: ₴{variant['current_price_profit']:.2f} (max {max(variant['current_price_profit'], variant['potential_profit']):.2f})")
            print(f"   RAPS Score: {variant['raps_score']:.4f}")
            print("")
            
        # Print variants without enough data at the end
        invalid_variants = [v for v in all_variants if not v.get('has_data', False)]
        if invalid_variants:
            print("\n=== VARIANTS WITH INSUFFICIENT DATA ===")
            for i, variant in enumerate(invalid_variants, 1):
                print(f"{i}. {variant.get('item_name', 'Unknown Item')}")
                if 'error' in variant: print(f"   Error: {variant['error']}")
                print("")
        
        return result
    finally:
        await analyzer.cleanup_all_analyzers()

async def shutdown(loop):
    """Shutdown gracefully by canceling all tasks."""
    print("\nShutting down gracefully...")
    tasks = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
    for task in tasks: task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()

# Main function to run the program interactively
async def main():
    # Setup signal handlers for clean shutdown
    loop = asyncio.get_event_loop()
    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown(loop)))
    except NotImplementedError:
        # Windows doesn't support add_signal_handler, use a different approach
        print("Signal handlers not supported on this platform, using fallback method")
    
    print("Enhanced Steam Market Price Analyzer (UAH Currency)")
    print("Input an item name to analyze its market price and profit potential.")
    
    while True:
        try:
            item_name = input("> ")
            if item_name.lower() in ['exit', 'quit', 'q']: break
            if not item_name.strip(): 
                print("Please enter a valid item name")
                continue
                
            await analyze_steam_item_by_name(item_name)
            if input("\nWould you like to analyze another item? (y/n) ").lower() != 'y': break
                
        except KeyboardInterrupt:
            print("\nExiting...")
            break
        except Exception as e:
            print(f"Error: {e}")
    
    # Allow a clean exit by running pending tasks
    await asyncio.sleep(0.5)
    
    # Close event loop properly
    tasks = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
    for task in tasks: task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
    asyncio.run(main())
"""
Ultra-fast async scraper with significant performance improvements.
Expected 2-5x speed improvement over current implementation.
"""
import asyncio
import aiohttp
import time
import json
import re
import logging
from bs4 import BeautifulSoup
from collections import defaultdict


# Configure logging
logger = logging.getLogger(__name__)


class AsyncLetterboxdScraper:
    """
    Ultra-fast async scraper with major performance optimizations:
    - True async/await with aiohttp for concurrent HTTP requests
    - Connection pooling and session reuse
    - Intelligent batching and request pipelining
    - Minimal DOM parsing with targeted selectors
    - In-memory data aggregation to reduce lock contention
    """
    
    def __init__(self, app_context):
        self.app_context = app_context
        self.session = None
        self.semaphore = None
        
        # Performance tuning parameters - aggressive for maximum speed
        self.max_concurrent_requests = min(50, self.app_context.config.max_threads * 4)  # Very aggressive
        self.request_delay = 0  # No delay between requests
        self.batch_delay = 0  # No delay between batches
        self.timeout = aiohttp.ClientTimeout(total=30, connect=10)
        
        # In-memory aggregation for better performance
        self.stats_aggregator = {
            'languages': defaultdict(int),
            'countries': defaultdict(int),
            'genres': defaultdict(int),
            'directors': defaultdict(int),
            'actors': defaultdict(int),
            'decades': defaultdict(int),
            'runtimes': []
        }
        
        # Progress tracking
        self.processed_count = 0

    async def _create_session(self):
        """Create optimized async session with connection pooling."""
        connector = aiohttp.TCPConnector(
            limit=100,  # High total connection pool size
            limit_per_host=25,  # Aggressive connections per host
            ttl_dns_cache=300,
            use_dns_cache=True,
            keepalive_timeout=60,
            enable_cleanup_closed=True
        )
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        }
        
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=self.timeout,
            headers=headers
        )
        
        # Semaphore to control concurrent requests
        self.semaphore = asyncio.Semaphore(self.max_concurrent_requests)

    async def _fetch_page(self, url):
        """Fetch a single page with aggressive performance settings."""
        async with self.semaphore:
            for attempt in range(3):  # Retry logic
                try:
                    async with self.session.get(url) as response:
                        if response.status == 200:
                            content = await response.read()
                            return content
                        else:
                            logger.warning(f"HTTP {response.status} for {url}")
                            
                except asyncio.TimeoutError:
                    if attempt < 2:  # Only log on final failure
                        logger.warning(f"Timeout for {url} (attempt {attempt + 1})")
                except Exception as e:
                    if attempt < 2:  # Only log on final failure
                        logger.warning(f"Request failed for {url}: {e} (attempt {attempt + 1})")
                
                if attempt < 2:  # Don't delay on last attempt
                    await asyncio.sleep(0.1 * (attempt + 1))  # Minimal backoff
            
            return None

    async def _scrape_film_page_async(self, url, total_films=0, start_time=0):
        """Ultra-fast async film page scraping with minimal parsing."""
        content = await self._fetch_page(url)
        if not content:
            return 0
        
        try:
            # Use lxml parser for speed, parse only what we need
            soup = BeautifulSoup(content, 'lxml')
            
            # Extract data with minimal DOM traversals
            film_data = self._extract_film_data_fast(soup)
            
            # Aggregate data immediately (no locking needed in async)
            self._aggregate_film_data(film_data)
            
            # Update progress after each film
            if total_films > 0 and start_time > 0:
                self.processed_count += 1
                elapsed_time = time.time() - start_time
                progress = (self.processed_count / total_films) * 100
                bar_length = 40
                filled = int(bar_length * self.processed_count / total_films)
                bar = '█' * filled + '░' * (bar_length - filled)
                remaining = total_films - self.processed_count
                
                # Calculate ETA based on current speed
                if self.processed_count > 0 and elapsed_time > 0:
                    speed = self.processed_count / elapsed_time
                    eta_seconds = remaining / speed if speed > 0 else 0
                    
                    # Format ETA as Xm Ys or just Xs
                    if eta_seconds < 60:
                        eta_str = f"{int(eta_seconds)}s"
                    else:
                        minutes = int(eta_seconds // 60)
                        seconds = int(eta_seconds % 60)
                        eta_str = f"{minutes}m{seconds}s"
                    
                    # Clear line first, then print progress
                    print(f"\r{' ' * 120}\r[{bar}] {progress:.1f}% | {self.processed_count}/{total_films} films | {remaining} remaining | ETA: {eta_str}", end='', flush=True)
                else:
                    print(f"\r{' ' * 120}\r[{bar}] {progress:.1f}% | {self.processed_count}/{total_films} films | {remaining} remaining", end='', flush=True)
            
            return film_data.get('runtime', 0)
            
        except Exception as e:
            logger.error(f"Error parsing {url}: {e}")
            return 0

    def _extract_film_data_fast(self, soup):
        """Ultra-fast data extraction with optimized selectors."""
        film_data = {
            'languages': set(),
            'countries': set(), 
            'genres': set(),
            'directors': set(),
            'actors': set(),
            'decade': None,
            'runtime': 0
        }
        
        # Year extraction (fastest method first)
        year = self._extract_year_optimized(soup)
        if year:
            film_data['decade'] = f"{year // 10 * 10}s"
        
        # Runtime extraction
        film_data['runtime'] = self._extract_runtime_optimized(soup)
        
        # Extract all metadata in one pass
        try:
            # Languages and countries from details section
            details = soup.select_one('#tab-details')
            if details:
                # Languages
                for a in details.select('.text-sluglist a[href*="/language/"]'):
                    lang = self._clean_text_fast(a.get_text())
                    if lang and lang != "No spoken language":
                        film_data['languages'].add("None" if lang == "No spoken language" else lang)
                
                # Countries  
                for a in details.select('a[href*="/country/"]'):
                    country = self._clean_text_fast(a.get_text())
                    if country:
                        film_data['countries'].add(country)
            
            # Genres
            genres = soup.select('#tab-genres a[href*="/genre/"]')
            for a in genres:
                genre = self._clean_text_fast(a.get_text())
                if genre:
                    film_data['genres'].add(genre.capitalize())
            
            # Directors (production masthead first, fastest)
            directors = soup.select('section.production-masthead .credits a[href*="/director/"]')
            for a in directors:
                director = self._clean_text_fast(a.get_text())
                if director:
                    film_data['directors'].add(director)
            
            # Actors (limit to avoid performance impact)
            actors = soup.select('#tab-cast .cast-list a.text-slug')[:20]  # Limit to first 20
            for a in actors:
                actor = self._clean_text_fast(a.get_text())
                if actor and not any(x in actor.lower() for x in ['show all', 'show ']):
                    film_data['actors'].add(actor)
                    
        except Exception as e:
            logger.error(f"Error in data extraction: {e}")
        
        return film_data

    def _extract_year_optimized(self, soup):
        """Optimized year extraction with early exits."""
        try:
            # Method 1: Release date link (fastest)
            date_link = soup.select_one('span.releasedate a')
            if date_link:
                match = re.search(r'(\d{4})', date_link.get_text())
                if match:
                    return int(match.group(1))
            
            # Method 2: JSON-LD (if needed)
            for script in soup.select('script[type="application/ld+json"]'):
                try:
                    data = json.loads(script.string or '{}')
                    if isinstance(data, dict):
                        # Check release event
                        if 'releasedEvent' in data and data['releasedEvent']:
                            date = data['releasedEvent'][0].get('startDate')
                            if date:
                                match = re.search(r'(\d{4})', str(date))
                                if match:
                                    return int(match.group(1))
                        
                        # Check other date fields
                        for field in ['dateCreated', 'datePublished']:
                            date = data.get(field)
                            if date:
                                match = re.search(r'(\d{4})', str(date))
                                if match:
                                    return int(match.group(1))
                except json.JSONDecodeError:
                    continue
                    
        except Exception:
            pass
        return None

    def _extract_runtime_optimized(self, soup):
        """Optimized runtime extraction."""
        try:
            footer = soup.select_one('.text-link.text-footer')
            if footer:
                match = re.search(r'(\d+)\s*min', footer.get_text(), re.I)
                if match:
                    return int(match.group(1))
        except Exception:
            pass
        return 0

    def _clean_text_fast(self, text):
        """Ultra-fast text cleaning."""
        if not text:
            return ""
        cleaned = text.strip()
        if ',' in cleaned:
            cleaned = cleaned.partition(',')[0]
        return cleaned

    def _aggregate_film_data(self, film_data):
        """Aggregate film data into global statistics."""
        # Aggregate all data types
        for lang in film_data['languages']:
            self.stats_aggregator['languages'][lang] += 1
        
        for country in film_data['countries']:
            self.stats_aggregator['countries'][country] += 1
            
        for genre in film_data['genres']:
            self.stats_aggregator['genres'][genre] += 1
            
        for director in film_data['directors']:
            self.stats_aggregator['directors'][director] += 1
            
        for actor in film_data['actors']:
            self.stats_aggregator['actors'][actor] += 1
            
        if film_data['decade']:
            self.stats_aggregator['decades'][film_data['decade']] += 1
            
        if film_data['runtime'] > 0:
            self.stats_aggregator['runtimes'].append(film_data['runtime'])

    async def _get_films_from_page_async(self, url):
        """Async film URL collection from page."""
        content = await self._fetch_page(url)
        if not content:
            return [], False
        
        try:
            soup = BeautifulSoup(content, 'lxml')
            base_url = "https://letterboxd.com"
            
            # Extract film URLs efficiently
            film_urls = []
            posters = soup.select('div.react-component[data-component-class="LazyPoster"]')
            
            for comp in posters[:72]:  # Letterboxd page limit
                link = comp.get('data-item-link') or ''
                slug = comp.get('data-item-slug') or ''
                
                if link:
                    film_urls.append(base_url + link)
                elif slug:
                    film_urls.append(f"{base_url}/film/{slug}/")
            
            # Check for next page
            next_link = soup.select_one('.paginate-next, .next')
            has_next = next_link is not None
            
            return film_urls, has_next
            
        except Exception as e:
            logger.error(f"Error parsing page {url}: {e}")
            return [], False

    async def scrape_user_profile_async(self, username):
        """Ultra-fast async user profile scraping."""
        try:
            await self._create_session()
            
            # Verify user exists
            test_url = f"https://letterboxd.com/{username}/films/"
            content = await self._fetch_page(test_url)
            if not content or b"Page not found" in content:
                logger.error(f"User '{username}' not found")
                return
            
            print(f"Collecting film URLs for user: {username}")
            start_time = time.time()
            
            # Collect all film URLs with async pagination
            all_film_urls = []
            page_num = 1
            
            while True:
                url = f"https://letterboxd.com/{username}/films/page/{page_num}/"
                film_urls, has_next = await self._get_films_from_page_async(url)
                
                all_film_urls.extend(film_urls)
                
                if not has_next or not film_urls:
                    break
                    
                page_num += 1
                await asyncio.sleep(self.request_delay)  # Rate limiting
            
            # Store URLs in app context
            self.app_context.stats_data.reset()
            for url in all_film_urls:
                self.app_context.stats_data.add_url(url)
            
            # Process all films concurrently with async
            print("Analyzing films with async processing...")
            analysis_start = time.time()
            
            # Reset progress counter
            self.processed_count = 0
            
            # Create async tasks for all films with total count and start time for ETA
            total_films = len(all_film_urls)
            tasks = [self._scrape_film_page_async(url, total_films, analysis_start) for url in all_film_urls]
            
            # Process in aggressive batches for maximum speed
            batch_size = 100  # Large batches
            runtime_list = []
            
            for i in range(0, len(tasks), batch_size):
                batch = tasks[i:i + batch_size]
                batch_results = await asyncio.gather(*batch, return_exceptions=True)
                
                # Filter out exceptions and collect runtimes
                for result in batch_results:
                    if isinstance(result, (int, float)) and result > 0:
                        runtime_list.append(result)
                
                # No delays between batches for maximum speed
            
            print()  # New line after progress bar
            
            analysis_time = time.time() - analysis_start
            total_time = time.time() - start_time
            
            # Transfer aggregated data to app context
            self._transfer_aggregated_data()
            
            # Calculate final statistics
            films_num = len(all_film_urls)
            hrs = sum(runtime_list) / 60
            dys = hrs / 24
            
            print(f"\nFilms analyzed: {films_num}")
            print(f"Total time: {total_time:.1f}s")
            print(f"Speed: {films_num/total_time:.1f} films/second")
            print(f"Time per film: {total_time/films_num:.3f}s")
            
            # Set meta data
            scraped_when = time.strftime("%d/%m/%Y", time.localtime())
            self.app_context.stats_data.set_meta_data(films_num, hrs, dys, scraped_when)
            
        except Exception as e:
            logger.error(f"Error in async scraping: {e}")
            raise
        finally:
            if self.session:
                await self.session.close()

    def _transfer_aggregated_data(self):
        """Transfer aggregated data to app context statistics."""
        stats = self.app_context.stats_data
        
        # Transfer dictionaries
        stats.lang_dict.update(self.stats_aggregator['languages'])
        stats.country_dict.update(self.stats_aggregator['countries'])
        stats.genre_dict.update(self.stats_aggregator['genres'])
        stats.director_dict.update(self.stats_aggregator['directors'])
        stats.actor_dict.update(self.stats_aggregator['actors'])
        stats.decade_dict.update(self.stats_aggregator['decades'])

    def scrape_user_profile(self, username):
        """Synchronous wrapper for async scraping."""
        return asyncio.run(self.scrape_user_profile_async(username))
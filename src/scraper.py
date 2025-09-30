"""
Enhanced scraper with performance improvements for faster analysis.
"""
import json
import re
import time
import requests
import concurrent.futures
import logging
from bs4 import BeautifulSoup
from collections import defaultdict
import threading


# Configure logging
logger = logging.getLogger(__name__)


class LetterboxdScraper:
    """
    Enhanced scraper with performance optimizations:
    - Connection pooling and session reuse
    - Batch data processing
    - Single DOM traversal for data extraction
    """
    
    def __init__(self, app_context):
        self.app_context = app_context
        self.session = None
        # Batch processing for reduced lock contention
        self.batch_data = []
        self.batch_lock = threading.Lock()
        self.batch_size = 50
    
    def _create_session(self):
        """Create an optimized requests session with connection pooling."""
        self.session = requests.Session()
        
        # Optimize session for performance
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,  # Connection pool size
            pool_maxsize=50,      # Max connections in pool
            max_retries=3,        # Retry failed requests
            pool_block=False      # Don't block on pool exhaustion
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            # Keep connections alive
            'Keep-Alive': 'timeout=30, max=100'
        })
    
    def _scrape_film_page_optimized(self, url_film_page):
        """Optimized film page scraping with reduced parsing overhead."""
        try:
            # Use shorter timeout for faster failure detection
            response = self.session.get(url_film_page, timeout=10)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Request failed for {url_film_page}: {e}")
            return 0
        
        # Use faster parser when possible
        soup = BeautifulSoup(response.content, 'lxml')
        
        # Batch data collection to reduce lock contention
        film_data = {
            'languages': set(),
            'countries': set(),
            'genres': set(),
            'directors': set(),
            'actors': set(),
            'decade': None,
            'runtime': 0
        }
        
        # Extract all data at once to minimize DOM traversals
        self._extract_all_film_data(soup, film_data)
        
        # Add to batch for bulk processing
        with self.batch_lock:
            self.batch_data.append(film_data)
            if len(self.batch_data) >= self.batch_size:
                self._process_batch()
        
        return film_data['runtime']
    
    def _extract_all_film_data(self, soup, film_data):
        """Extract all film data in a single DOM traversal for efficiency."""
        # Get year/decade first
        year_found = self._extract_year_fast(soup)
        if year_found:
            film_data['decade'] = f"{year_found // 10 * 10}s"
        
        # Get runtime
        film_data['runtime'] = self._extract_runtime_fast(soup)
        
        # Extract all text data in one pass to minimize DOM queries
        try:
            # Details section (languages, countries)
            details_section = soup.select_one('#tab-details')
            if details_section:
                # Languages
                for a_tag in details_section.select('.text-sluglist a[href^="/films/language/"]'):
                    lang = self._clean_text(a_tag.text)
                    if lang and lang != "No spoken language":
                        film_data['languages'].add("None" if lang == "No spoken language" else lang)
                
                # Countries
                for a_tag in details_section.select('a[href^="/films/country/"]'):
                    country = self._clean_text(a_tag.text)
                    if country:
                        film_data['countries'].add(country)
            
            # Genres
            genres_section = soup.select_one('#tab-genres')
            if genres_section:
                for a_tag in genres_section.select('a[href^="/films/genre/"]'):
                    genre = self._clean_text(a_tag.text)
                    if genre:
                        film_data['genres'].add(genre.capitalize())
            
            # Directors
            credits = soup.select_one('section.production-masthead .details .credits')
            if credits:
                for a_tag in credits.select('a[href^="/director/"]'):
                    director = self._clean_text(a_tag.text)
                    if director:
                        film_data['directors'].add(director)
            
            # Actors
            cast_items = soup.select('#tab-cast .cast-list a.text-slug, .cast-list.text-sluglist a.text-slug')
            for a_tag in cast_items:
                actor = self._clean_text(a_tag.text)
                if actor and not any(x in actor.lower() for x in ['show all', 'show ']):
                    film_data['actors'].add(actor)
        
        except Exception as e:
            logger.error(f"Error extracting film data: {e}")
            # Continue processing even if this film fails
    
    def _clean_text(self, text):
        """Clean and process text."""
        if not text:
            return None
        cleaned = text.strip()
        if ',' in cleaned:
            cleaned = cleaned.partition(',')[0]
        return cleaned if cleaned else None
    
    def _extract_year_fast(self, soup):
        """Optimized year extraction with early termination."""
        try:
            # Try visible date first (fastest)
            releasedate_link = soup.select_one('span.releasedate a')
            if releasedate_link and releasedate_link.text:
                match = re.search(r'(\d{4})', releasedate_link.text)
                if match:
                    return int(match.group(1))
            
            # Fall back to JSON-LD if needed
            script_tags = soup.select('script[type="application/ld+json"]')
            for tag in script_tags[:2]:  # Limit search to first 2 scripts
                try:
                    data = json.loads(tag.string or '{}')
                    if isinstance(data, dict) and data.get('@type') in ('Movie', 'VideoObject'):
                        # Check release events
                        if isinstance(data.get('releasedEvent'), list) and data['releasedEvent']:
                            dt = data['releasedEvent'][0].get('startDate')
                            if dt:
                                match = re.search(r'(\d{4})', str(dt))
                                if match:
                                    return int(match.group(1))
                        
                        # Check date fields
                        for field in ['dateCreated', 'datePublished']:
                            dt = data.get(field)
                            if dt:
                                match = re.search(r'(\d{4})', str(dt))
                                if match:
                                    return int(match.group(1))
                except json.JSONDecodeError:
                    continue
        except Exception:
            pass
        return None
    
    def _extract_runtime_fast(self, soup):
        """Optimized runtime extraction."""
        try:
            # Try footer first
            footer = soup.select_one('.text-link.text-footer')
            if footer:
                match = re.search(r'(\d+)\s*min', footer.get_text(' '), re.I)
                if match:
                    return int(match.group(1))
        except Exception:
            pass
        return 0
    
    def _process_batch(self):
        """Process accumulated batch data to reduce lock contention."""
        if not self.batch_data:
            return
        
        # Aggregate all batch data
        aggregated = {
            'languages': defaultdict(int),
            'countries': defaultdict(int),
            'genres': defaultdict(int),
            'directors': defaultdict(int),
            'actors': defaultdict(int),
            'decades': defaultdict(int)
        }
        
        for film_data in self.batch_data:
            for lang in film_data['languages']:
                aggregated['languages'][lang] += 1
            for country in film_data['countries']:
                aggregated['countries'][country] += 1
            for genre in film_data['genres']:
                aggregated['genres'][genre] += 1
            for director in film_data['directors']:
                aggregated['directors'][director] += 1
            for actor in film_data['actors']:
                aggregated['actors'][actor] += 1
            if film_data['decade']:
                aggregated['decades'][film_data['decade']] += 1
        
        # Single lock acquisition for all updates
        with self.app_context.stats_data.lock:
            for lang, count in aggregated['languages'].items():
                self.app_context.stats_data.lang_dict[lang] = self.app_context.stats_data.lang_dict.get(lang, 0) + count
            for country, count in aggregated['countries'].items():
                self.app_context.stats_data.country_dict[country] = self.app_context.stats_data.country_dict.get(country, 0) + count
            for genre, count in aggregated['genres'].items():
                self.app_context.stats_data.genre_dict[genre] = self.app_context.stats_data.genre_dict.get(genre, 0) + count
            for director, count in aggregated['directors'].items():
                self.app_context.stats_data.director_dict[director] = self.app_context.stats_data.director_dict.get(director, 0) + count
            for actor, count in aggregated['actors'].items():
                self.app_context.stats_data.actor_dict[actor] = self.app_context.stats_data.actor_dict.get(actor, 0) + count
            for decade, count in aggregated['decades'].items():
                self.app_context.stats_data.decade_dict[decade] += count
        
        self.batch_data.clear()
    
    def _get_films_from_page_optimized(self, url_table_page):
        """Optimized film URL extraction with pagination detection."""
        try:
            response = self.session.get(url_table_page, timeout=15)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error(f"Failed to get films page {url_table_page}: {e}")
            return 0, False
        
        soup = BeautifulSoup(response.text, 'lxml')
        url_ltbxd = "https://letterboxd.com"
        
        # Get posters
        posters = soup.select('div.react-component[data-component-class="LazyPoster"]')
        count = 0
        
        for comp in posters:
            link = comp.get('data-item-link') or ''
            slug = comp.get('data-item-slug') or ''
            film_url = None
            
            if link:
                film_url = url_ltbxd + link
            elif slug:
                film_url = f"{url_ltbxd}/film/{slug}/"
            
            if film_url:
                self.app_context.stats_data.add_url(film_url)
                count += 1
                if count >= 72:  # Letterboxd's page limit
                    break
        
        # Check if there's a next page link
        next_link = (
            soup.select_one('div.pagination a.next') or
            soup.select_one('a.next[rel="next"]') or
            soup.select_one('div.paginate-nextprev a.next')
        )
        has_next_page = next_link is not None
        
        return count, has_next_page
    
    def scrape_user_profile(self, username):
        """Optimized profile scraping with performance improvements."""
        self.app_context.stats_data.reset()
        self._create_session()
        
        print("Analyzing user:", username)
        
        # Verify user exists
        test_url = f"https://letterboxd.com/{username}/films/"
        try:
            r = self.session.get(test_url, timeout=10)
            if "Sorry, we can't find the page" in r.text:
                logger.error(f"User '{username}' not found")
                return None
        except requests.RequestException as e:
            logger.error(f"Error verifying user: {e}")
            return None
        
        print("Collecting film URLs...")
        start_time = time.time()
        
        # Collect all film URLs first
        page_num = 1
        
        while True:
            url = f"https://letterboxd.com/{username}/films/page/{page_num}/"
            films_found, has_next_page = self._get_films_from_page_optimized(url)
            
            # Use the same pagination logic as original scraper
            if not has_next_page:
                break
            
            page_num += 1
            
            # Safety break for very large collections
            if page_num > 1000:
                print("Reached maximum page limit (1000)")
                break
        
        collection_time = time.time() - start_time
        print(f"Found {len(self.app_context.stats_data.url_list)} films in {collection_time:.1f}s")
        
        if not self.app_context.stats_data.url_list:
            logger.warning("No films found for user")
            return None
        
        # Process films with optimized threading
        print("Analyzing films...")
        analysis_start = time.time()
        
        # Use adaptive thread count based on number of films
        max_workers = min(self.app_context.config.max_threads, len(self.app_context.stats_data.url_list))
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Add progress tracking
            futures = [executor.submit(self._scrape_film_page_optimized, url) 
                      for url in self.app_context.stats_data.url_list]
            
            runtime_list = []
            completed = 0
            
            for future in concurrent.futures.as_completed(futures):
                try:
                    runtime = future.result()
                    runtime_list.append(runtime)
                    completed += 1
                    
                    # Progress feedback every 100 films
                    if completed % 100 == 0:
                        print(f"Analyzed {completed}/{len(self.app_context.stats_data.url_list)} films...")
                        
                except Exception as e:
                    logger.warning(f"Failed to process film: {e}")
                    runtime_list.append(0)
        
        # Process any remaining batch data
        with self.batch_lock:
            if self.batch_data:
                self._process_batch()
        
        total_time = time.time() - start_time
        
        # Calculate statistics
        films_num = len(self.app_context.stats_data.url_list)
        hrs = sum(runtime_list) / 60
        dys = hrs / 24
        
        print(f"\nFilms analyzed: {films_num}")
        print(f"Total time: {total_time:.1f}s")
        
        # Set meta data
        try:
            scraped_when = time.strftime("%d/%m/%Y", time.localtime())
        except Exception:
            scraped_when = ""
        
        self.app_context.stats_data.set_meta_data(films_num, hrs, dys, scraped_when)
        
        return {
            'films_num': films_num,
            'total_hours': hrs,
            'total_days': dys,
            'username': username,
            'scraped_at': scraped_when
        }
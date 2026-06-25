"""
TMDB API-based scraper.
Reads exported CSV data from Letterboxd and uses the TMDB API to fetch film metadata.
"""
import sys
import csv
import time
import logging
import requests
from collections import defaultdict


# Configure logging
logger = logging.getLogger(__name__)


class TMDbScraper:
    """
    TMDB API scraper that reads Letterboxd CSV exports and fetches film data
    using The Movie Database API.
    
    Supports watched.csv, ratings.csv, comments.csv, diary.csv, reviews.csv
    with columns like Date, Name, Year, Letterboxd URI.
    """
    
    TMDB_BASE_URL = "https://api.themoviedb.org/3"
    TMDB_IMAGE_BASE = "https://image.tmdb.org/p/t/p/original"
    
    # Rate limit handling
    MAX_RETRIES = 5
    INITIAL_RETRY_DELAY = 2.0  # seconds
    
    def __init__(self, app_context, progress_callback=None):
        self.app_context = app_context
        self.session = requests.Session()
        self.progress_callback = progress_callback
        self.session.headers.update({
            'User-Agent': 'LePrAn (Letterboxd Profile Analyzer) - Python Requests',
            'Accept': 'application/json',
        })
        
        # Performance tuning
        self.batch_delay = 0.5  # small delay between batches to stay under rate limits
        
        # In-memory data aggregation
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
    
    def _get_tmdb_token(self):
        """Get the TMDB access token from config."""
        return self.app_context.config.tmdb_access_token
    
    def _make_tmdb_request(self, endpoint, params=None):
        """Make a TMDB API request with rate limit handling."""
        token = self._get_tmdb_token()
        if not token:
            logger.error("TMDB access token not found. Please set TMDB_ACCESS_TOKEN in .env")
            return None
        
        url = f"{self.TMDB_BASE_URL}{endpoint}"
        params = params or {}
        params['api_key'] = token
        
        retry_delay = self.INITIAL_RETRY_DELAY
        
        for attempt in range(self.MAX_RETRIES):
            try:
                response = self.session.get(url, params=params, timeout=30)
                
                if response.status_code == 200:
                    return response.json()
                
                elif response.status_code == 429:
                    # Rate limited - wait and retry
                    logger.warning(f"TMDB rate limit hit (429). Waiting {retry_delay}s before retry (attempt {attempt + 1}/{self.MAX_RETRIES})")
                    
                    # Check for Retry-After header
                    retry_after = response.headers.get('Retry-After')
                    if retry_after:
                        wait_time = float(retry_after)
                    else:
                        wait_time = retry_delay
                    
                    time.sleep(wait_time)
                    retry_delay *= 2  # Exponential backoff
                    continue
                
                elif response.status_code == 401:
                    logger.error("TMDB authentication failed. Check your access token.")
                    return None
                
                elif response.status_code == 404:
                    # Film not found in TMDB
                    return {'results': []}
                
                else:
                    logger.warning(f"TMDB API error {response.status_code} for {url}: {response.text[:200]}")
                    if attempt < self.MAX_RETRIES - 1:
                        time.sleep(retry_delay)
                        retry_delay *= 2
                    
            except requests.exceptions.Timeout:
                logger.warning(f"TMDB request timeout (attempt {attempt + 1})")
                if attempt < self.MAX_RETRIES - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2
            
            except requests.exceptions.ConnectionError as e:
                logger.warning(f"TMDB connection error (attempt {attempt + 1}): {e}")
                if attempt < self.MAX_RETRIES - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2
        
        logger.error(f"Failed after {self.MAX_RETRIES} attempts")
        return None
    
    def search_movie(self, name, year=None):
        """Search for a movie by name and optionally year."""
        params = {'query': name}
        if year:
            params['year'] = year
        
        data = self._make_tmdb_request('/search/movie', params)
        
        if data and 'results' in data:
            results = data['results']
            # Prefer exact year match, then earliest release date
            if year:
                for r in results:
                    rel_year = r.get('release_date', '')[:4]
                    if rel_year == str(year):
                        return r
            
            # Return the first result (usually most relevant)
            if results:
                return results[0]
        
        return None
    
    def get_movie_details(self, tmdb_id):
        """Get detailed movie information by TMDB ID."""
        data = self._make_tmdb_request(f'/movie/{tmdb_id}', {
            'append_to_response': 'credits'
        })
        return data
    
    def _extract_film_data(self, movie_data, credits_data=None):
        """Extract film metadata from TMDB API response."""
        film_data = {
            'languages': [],
            'countries': [],
            'genres': [],
            'directors': [],
            'actors': [],
            'decade': None,
            'runtime': 0
        }
        
        if not movie_data:
            return film_data
        
        # Genres
        for genre in movie_data.get('genres', []):
            g = genre.get('name', '').strip()
            if g:
                film_data['genres'].append(g.capitalize())
        
        # Production countries
        for country in movie_data.get('production_countries', []):
            c = country.get('name', '').strip()
            if c:
                film_data['countries'].append(c)
        
        # Spoken languages
        for lang in movie_data.get('spoken_languages', []):
            l = lang.get('name', '').strip()
            if l and l != "":
                film_data['languages'].append(l)
        
        # Runtime
        runtime = movie_data.get('runtime')
        if runtime and isinstance(runtime, (int, float)) and runtime > 0:
            film_data['runtime'] = int(runtime)
        
        # Release date / decade
        release_date = movie_data.get('release_date', '')
        if release_date:
            try:
                year = int(release_date[:4])
                film_data['decade'] = f"{year // 10 * 10}s"
            except (ValueError, IndexError):
                pass
        
        # Credits (directors and actors)
        if credits_data and 'credits' in credits_data:
            credits = credits_data['credits']
            
            # Directors - take first few
            for crew in credits.get('crew', []):
                if crew.get('job') == 'Director':
                    d = crew.get('name', '').strip()
                    if d:
                        film_data['directors'].append(d)
                        break  # Usually only one director listed as primary
            
            # Cast - limit to first 20
            for cast in credits.get('cast', [])[:20]:
                a = cast.get('name', '').strip()
                if a:
                    film_data['actors'].append(a)
        
        return film_data
    
    def _aggregate_film_data(self, film_data):
        """Aggregate film data into global statistics."""
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
    
    def parse_csv_file(self, csv_path):
        """Parse a Letterboxd-exported CSV file and extract (Name, Year) tuples.
        
        Supports formats:
        - watched.csv / watchlist.csv: Date,Name,Year,Letterboxd URI
        - ratings.csv: Date,Name,Year,Letterboxd URI,Rating
        - diary.csv: Date,Name,Year,Letterboxd URI (diary entries)
        
        Returns a list of (name, year_str) tuples.
        """
        films = []
        
        try:
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                # Detect delimiter (CSV might use comma or semicolon)
                sample = f.read(4096)
                f.seek(0)
                
                sniffer = csv.Sniffer()
                try:
                    dialect = sniffer.sniff(sample, delimiters=',;\t|')
                    delimiter = dialect.delimiter
                except csv.Error:
                    delimiter = ','
                
                reader = csv.reader(f, delimiter=delimiter)
                
                # Read header
                try:
                    header = [h.strip().lower() for h in next(reader)]
                except StopIteration:
                    logger.warning("Empty CSV file")
                    return []
                
                # Find column indices
                name_idx = None
                year_idx = None
                
                for i, col in enumerate(header):
                    if col in ('name', 'title', 'film'):
                        name_idx = i
                    elif col in ('year', 'release_year'):
                        year_idx = i
                
                # If no explicit year column, try to extract from name or use URI
                if name_idx is None:
                    logger.warning(f"Could not find 'Name' column in CSV headers: {header}")
                    return []
                
                for row_num, row in enumerate(reader, start=2):
                    if len(row) <= max(name_idx, year_idx if year_idx else 0):
                        continue
                    
                    name = row[name_idx].strip()
                    year = ''
                    if year_idx is not None and year_idx < len(row):
                        year = row[year_idx].strip()
                    
                    if name:
                        films.append((name, year))
        
        except FileNotFoundError:
            logger.error(f"CSV file not found: {csv_path}")
        except Exception as e:
            logger.error(f"Error parsing CSV file {csv_path}: {e}")
        
        return films
    
    def scrape_csv_file(self, csv_path):
        """Main entry point: scrape film data from a Letterboxd CSV export using TMDB API."""
        token = self._get_tmdb_token()
        if not token:
            logger.error("TMDB access token not configured. Please add TMDB_ACCESS_TOKEN to your .env file.")
            print("\n[ERROR] TMDB access token not found in config!")
            return
        
        films = self.parse_csv_file(csv_path)
        if not films:
            print("[INFO] No films found in CSV file.")
            return
        
        total_films = len(films)
        print(f"\nFound {total_films} films in CSV file: {csv_path}")
        print("Fetching data from TMDB API...")
        
        start_time = time.time()
        self.processed_count = 0
        
        # Reset aggregator
        for key in self.stats_aggregator:
            if isinstance(self.stats_aggregator[key], defaultdict):
                self.stats_aggregator[key] = defaultdict(int)
            elif key == 'runtimes':
                self.stats_aggregator['runtimes'] = []
        
        batch_count = 0
        
        for i, (name, year) in enumerate(films):
            # Search for the movie on TMDB
            tmdb_movie = self.search_movie(name, year if year else None)
            
            film_data = {'languages': [], 'countries': [], 'genres': [], 
                        'directors': [], 'actors': [], 'decade': None, 'runtime': 0}
            
            if tmdb_movie:
                tmdb_id = tmdb_movie.get('id')
                if tmdb_id:
                    details = self.get_movie_details(tmdb_id)
                    film_data = self._extract_film_data(details, details)
            
            # Aggregate data
            self._aggregate_film_data(film_data)
            
            # Update progress
            self.processed_count += 1
            
            if total_films > 0:
                elapsed_time = time.time() - start_time
                progress = (self.processed_count / total_films) * 100
                bar_length = 40
                filled = int(bar_length * self.processed_count / total_films)
                bar = '█' * filled + '░' * (bar_length - filled)
                remaining = total_films - self.processed_count
                
                if self.processed_count > 0 and elapsed_time > 0:
                    speed = self.processed_count / elapsed_time
                    eta_seconds = remaining / speed if speed > 0 else 0
                    
                    if eta_seconds < 60:
                        eta_str = f"{int(eta_seconds)}s"
                    else:
                        minutes = int(eta_seconds // 60)
                        seconds = int(eta_seconds % 60)
                        eta_str = f"{minutes}m{seconds}s"
                    
                    sys.stdout.write(f"\r[{bar}] {progress:.1f}% | {self.processed_count}/{total_films} films | {remaining} remaining | ETA: {eta_str}")
                    sys.stdout.flush()
                
                # Notify frontend of progress via callback
                if self.progress_callback:
                    status_msg = f"Processing films ({self.processed_count}/{total_films})"
                    self.progress_callback(int(progress), status_msg)
                    
                    # Set detailed progress stats for frontend display (ETA, speed)
                    if hasattr(self.progress_callback, '__self__'):
                        api = self.progress_callback.__self__
                        if hasattr(api, 'set_progress_stats'):
                            api.set_progress_stats(self.processed_count, total_films, speed, eta_seconds)
            
            # Small delay between requests to avoid rate limiting
            if i % 10 == 9:
                time.sleep(self.batch_delay)
        
        print()  # New line after progress bar
        
        analysis_time = time.time() - start_time
        films_count = total_films
        hrs = sum(self.stats_aggregator['runtimes']) / 60 if self.stats_aggregator['runtimes'] else 0
        dys = hrs / 24
        
        print(f"\nFilms processed: {films_count}")
        print(f"Total time: {analysis_time:.1f}s")
        print(f"Speed: {films_count/analysis_time:.1f} films/second")
        
        # Transfer aggregated data to app context
        self._transfer_aggregated_data()
        
        # Set meta data
        scraped_when = time.strftime("%d/%m/%Y", time.localtime())
        self.app_context.stats_data.set_meta_data(films_count, hrs, dys, scraped_when)
    
    def _transfer_aggregated_data(self):
        """Transfer aggregated data to app context statistics."""
        stats = self.app_context.stats_data
        
        # Clear existing data first (for re-scraping support)
        stats.lang_dict.clear()
        stats.country_dict.clear()
        stats.genre_dict.clear()
        stats.director_dict.clear()
        stats.actor_dict.clear()
        stats.decade_dict.clear()
        
        # Transfer dictionaries
        for k, v in self.stats_aggregator['languages'].items():
            stats.lang_dict[k] = v
        for k, v in self.stats_aggregator['countries'].items():
            stats.country_dict[k] = v
        for k, v in self.stats_aggregator['genres'].items():
            stats.genre_dict[k] = v
        for k, v in self.stats_aggregator['directors'].items():
            stats.director_dict[k] = v
        for k, v in self.stats_aggregator['actors'].items():
            stats.actor_dict[k] = v
        for k, v in self.stats_aggregator['decades'].items():
            stats.decade_dict[k] = v
    
    def scrape_user_profile(self, csv_path):
        """Synchronous entry point - compatible with existing scraper interface."""
        self.scrape_csv_file(csv_path)
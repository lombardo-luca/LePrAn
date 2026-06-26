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
        self.batch_delay = 0.2  # small delay between batches to stay under rate limits
        
        # In-memory data aggregation
        self.stats_aggregator = {
            'languages': defaultdict(int),
            'countries': defaultdict(int),
            'genres': defaultdict(int),
            'directors': defaultdict(int),
            'actors': defaultdict(int),
            'decades': defaultdict(int),
            'runtimes': [],
            'film_budgets': defaultdict(float),
            'film_boxoffices': defaultdict(float)
        }
        
        # Progress tracking
        self.processed_count = 0
        
        # Match statistics tracking
        self.match_stats = {
            'total': 0,
            'fast_path': 0,
            'fallback_path': 0,
            'no_match': 0,
            'low_confidence': 0,
            'year_deltas': [],
            'confidences': []
        }
    
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
    
    def _calculate_match_confidence(self, tmdb_movie, letterboxd_year):
        """Calculate match confidence score based on year proximity and title similarity.
        
        Args:
            tmdb_movie: TMDB movie dict with 'release_date' and 'title'
            letterboxd_year: Year string from Letterboxd (e.g., '1945')
        
        Returns:
            tuple: (confidence: float, year_delta: int)
        """
        if not letterboxd_year:
            return (0.95, 0)  # No year info = moderate confidence
        
        try:
            lb_year = int(letterboxd_year)
        except (ValueError, TypeError):
            return (0.95, 0)
        
        tmdb_year_str = tmdb_movie.get('release_date', '')[:4]
        if not tmdb_year_str or tmdb_year_str == '':
            return (0.7, 999)  # No release date = lower confidence
        
        try:
            tmdb_year = int(tmdb_year_str)
        except (ValueError, TypeError):
            return (0.7, 999)
        
        year_delta = abs(tmdb_year - lb_year)
        
        # Confidence scoring based on year delta
        if year_delta == 0:
            confidence = 1.0
        elif year_delta == 1:
            confidence = 0.95
        elif year_delta <= 2:
            confidence = 0.85
        elif year_delta <= 5:
            confidence = 0.7
        else:
            confidence = 0.4  # Low confidence for large year gaps
        
        return (confidence, year_delta)
    
    def _select_best_match(self, results, letterboxd_year, letterboxd_title):
        """Select the best match from TMDB search results using hierarchical strategy.
        
        Priority:
        1. Exact year match → highest popularity
        2. Closest year proximity → highest popularity as tiebreaker
        3. TMDB relevance ranking (fallback when no year info)
        
        Args:
            results: List of TMDB movie dicts from search
            letterboxd_year: Year string from Letterboxd
            letterboxd_title: Title string from Letterboxd
        
        Returns:
            Best matching TMDB movie dict, or None if no good match
        """
        if not results:
            return None
        
        # If no year info, trust TMDB's relevance ranking
        if not letterboxd_year:
            return results[0]
        
        # Collect all candidates with metadata
        candidates = []
        for r in results:
            rel_year_str = r.get('release_date', '')[:4]
            try:
                rel_year = int(rel_year_str)
                delta = abs(rel_year - int(letterboxd_year))
            except (ValueError, TypeError):
                delta = 999
            
            popularity = r.get('popularity', 0) or 0
            candidates.append({
                'movie': r,
                'year': rel_year_str,
                'delta': delta,
                'popularity': popularity
            })
        
        # TIER 1: Exact year match - select by highest popularity
        exact_matches = [c for c in candidates if c['delta'] == 0]
        if exact_matches:
            exact_matches.sort(key=lambda x: x['popularity'], reverse=True)
            return exact_matches[0]['movie']
        
        # TIER 2: Close year match (±2 years) - closest year first, then popularity
        close_matches = [c for c in candidates if 1 <= c['delta'] <= 2]
        if close_matches:
            # Sort by delta ASC, then popularity DESC
            close_matches.sort(key=lambda x: (x['delta'], -x['popularity']))
            return close_matches[0]['movie']
        
        # TIER 3: Fallback - closest year among all remaining, with popularity tiebreaker
        # Sort by delta ASC, then popularity DESC
        candidates.sort(key=lambda x: (x['delta'], -x['popularity']))
        return candidates[0]['movie']
    
    def _build_candidate_list(self, results, letterboxd_year):
        """Build a list of candidate matches with metadata for logging.
        
        Args:
            results: List of TMDB movie dicts from search
            letterboxd_year: Year string from Letterboxd
        
        Returns:
            List of candidate dicts with metadata
        """
        candidates = []
        for r in results:
            rel_year_str = r.get('release_date', '')[:4]
            try:
                rel_year = int(rel_year_str)
                delta = abs(rel_year - int(letterboxd_year)) if letterboxd_year else 0
            except (ValueError, TypeError):
                delta = 999
            
            popularity = r.get('popularity', 0) or 0
            candidates.append({
                'title': r.get('title', ''),
                'year': rel_year_str,
                'popularity': popularity,
                'delta': delta
            })
        
        return candidates
    
    def _select_best_match_from_candidates(self, candidates):
        """Select best match from pre-computed candidate list.
        
        Priority:
        1. Exact year match → highest popularity
        2. Closest year proximity → highest popularity as tiebreaker
        
        Args:
            candidates: List of candidate dicts with 'movie', 'delta', 'popularity'
        
        Returns:
            Best matching TMDB movie dict
        """
        if not candidates:
            return None
        
        # Exact year match - highest popularity
        exact = [c for c in candidates if c['delta'] == 0]
        if exact:
            exact.sort(key=lambda x: x['popularity'], reverse=True)
            return exact[0]['movie']
        
        # Closest year - highest popularity
        candidates.sort(key=lambda x: (x['delta'], -x['popularity']))
        return candidates[0]['movie']
    
    def search_movie(self, name, year=None):
        """3-tier hierarchical TMDB search with strict matching and logging.
        
        TIER 1 (PRIMARY): Query + exact year match → highest popularity
        TIER 2 (SECONDARY): Query + ±1-2 years → closest year first, then popularity
        TIER 3 (FALLBACK): Query without year → strict penalty, flag ambiguous
        
        Args:
            name: Movie title
            year: Optional year string
        
        Returns:
            tuple: (tmdb_movie_dict_or_None, match_info_dict)
                match_info contains: confidence, year_delta, used_fallback, low_confidence,
                    match_tier, ambiguous, candidates
        """
        match_info = {
            'confidence': 0.0,
            'year_delta': 0,
            'used_fallback': False,
            'low_confidence': False,
            'match_tier': 'none',
            'ambiguous': False,
            'candidates': []
        }
        
        # --- TIER 1: PRIMARY — Exact year match, highest popularity ---
        if year:
            params = {'query': name, 'year': year}
            data = self._make_tmdb_request('/search/movie', params)
            
            if data and 'results' in data and len(data['results']) > 0:
                # Build candidate list for logging (used by fallback/nearest-tier if needed)
                candidates = self._build_candidate_list(data['results'], year)
                match_info['candidates'] = candidates
                
                # Select best exact-year match by popularity
                movie = self._select_best_match(data['results'], year, name)
                
                if movie:
                    conf, delta = self._calculate_match_confidence(movie, year)
                    match_info['confidence'] = conf
                    match_info['year_delta'] = delta
                    match_info['used_fallback'] = False
                    match_info['match_tier'] = 'exact'
                    
                    # No logging for exact matches — they are the expected/normal case
                    
                    return movie, match_info
        
        # --- TIER 2: SECONDARY — ±1-2 years, closest year first ---
        if year:
            # Try ±2 year window: year-2, year-1, year+1, year+2
            year_offsets = [-2, -1, 1, 2]
            tier2_results = []
            
            for offset in year_offsets:
                offset_year = int(year) + offset
                params = {'query': name, 'year': str(offset_year)}
                data = self._make_tmdb_request('/search/movie', params)
                
                if data and 'results' in data and len(data['results']) > 0:
                    tier2_results.extend(data['results'])
            
            if tier2_results:
                # Build candidate list from all tier 2 results
                candidates = self._build_candidate_list(tier2_results, year)
                match_info['candidates'] = candidates
                
                # Select best match: closest year first, then popularity
                movie = self._select_best_match(tier2_results, year, name)
                
                if movie:
                    conf, delta = self._calculate_match_confidence(movie, year)
                    match_info['confidence'] = conf
                    match_info['year_delta'] = delta
                    match_info['used_fallback'] = False
                    match_info['match_tier'] = 'nearest-year'
                    
                    logger.info(
                        f"[MATCH] '{name}' ({year}) → TMDB: '{movie.get('title', '')}' "
                        f"({movie.get('release_date', '')[:4]}) [nearest-year, delta: {delta}, confidence: {conf:.2f}]"
                    )
                    return movie, match_info
        
        # --- TIER 3: FALLBACK — No year filter, strict penalty ---
        params = {'query': name}
        data = self._make_tmdb_request('/search/movie', params)
        
        if data and 'results' in data and len(data['results']) > 0:
            # Build candidate list for logging
            candidates = self._build_candidate_list(data['results'], year)
            match_info['candidates'] = candidates
            
            # Select best match: closest year, then popularity
            movie = self._select_best_match(data['results'], year, name)
            
            if movie:
                conf, delta = self._calculate_match_confidence(movie, year)
                match_info['confidence'] = conf
                match_info['year_delta'] = delta
                match_info['used_fallback'] = True
                match_info['match_tier'] = 'fallback'
                
                # Check for ambiguity (delta > 10 years)
                if delta > 10:
                    match_info['ambiguous'] = True
                    match_info['low_confidence'] = True
                    logger.warning(
                        f"[AMBIGUOUS] '{name}' (Letterboxd: {year or '?'}) → "
                        f"TMDB: '{movie.get('title', '')}' ({movie.get('release_date', '')[:4]}) "
                        f"[fallback, delta: {delta} years] "
                        f"⚠️ Requires manual review — large year difference"
                    )
                elif delta > 5:
                    match_info['low_confidence'] = True
                    logger.warning(
                        f"[LOW CONFIDENCE] '{name}' (Letterboxd: {year or '?'}) → "
                        f"TMDB: '{movie.get('title', '')}' ({movie.get('release_date', '')[:4]}) "
                        f"[fallback, delta: {delta} years]"
                    )
                else:
                    logger.info(
                        f"[MATCH] '{name}' (Letterboxd: {year or '?'}) → "
                        f"TMDB: '{movie.get('title', '')}' ({movie.get('release_date', '')[:4]}) "
                        f"[fallback, delta: {delta}, confidence: {conf:.2f}]"
                    )
                
                return movie, match_info
        
        # No match found — always log to prevent silent failures
        logger.warning(
            f"[NO MATCH] '{name}' ({year or '?'}) — film excluded from metadata aggregation"
        )
        return None, match_info
    
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
            'runtime': 0,
            'budget': 0,
            'box_office': 0
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
        # Use english_name from TMDB to ensure consistent English display across the entire UI
        for lang in movie_data.get('spoken_languages', []):
            l = lang.get('english_name', lang.get('name', '')).strip()
            if l and l != "":
                film_data['languages'].append(l)
        
        # Runtime
        runtime = movie_data.get('runtime')
        if runtime and isinstance(runtime, (int, float)) and runtime > 0:
            film_data['runtime'] = int(runtime)
        
        # Budget
        budget = movie_data.get('budget')
        if budget and isinstance(budget, (int, float)) and budget > 0:
            film_data['budget'] = int(budget)
        
        # Box office revenue
        box_office = movie_data.get('revenue')
        if box_office and isinstance(box_office, (int, float)) and box_office > 0:
            film_data['box_office'] = int(box_office)
        
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
            
            # Directors - collect ALL directors (symmetric with actors/genres multi-value behavior)
            for crew in credits.get('crew', []):
                if crew.get('job') == 'Director':
                    d = crew.get('name', '').strip()
                    if d:
                        film_data['directors'].append(d)
            
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
        
        # Financial data - aggregate by film name (stored from caller context)
        # Note: budget/box_office are stored per-film and transferred by the caller
    
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
            # Hybrid search: (movie, match_info) tuple
            tmdb_movie, match_info = self.search_movie(name, year if year else None)
            
            # Track match statistics
            self.match_stats['total'] += 1
            if match_info['used_fallback']:
                self.match_stats['fallback_path'] += 1
            else:
                self.match_stats['fast_path'] += 1
            if tmdb_movie is None:
                self.match_stats['no_match'] += 1
            if match_info['low_confidence']:
                self.match_stats['low_confidence'] += 1
            if match_info['year_delta'] < 999:
                self.match_stats['year_deltas'].append(match_info['year_delta'])
            self.match_stats['confidences'].append(match_info['confidence'])
            
            film_data = {'languages': [], 'countries': [], 'genres': [], 
                        'directors': [], 'actors': [], 'decade': None, 'runtime': 0,
                        'budget': 0, 'box_office': 0}
            
            if tmdb_movie:
                tmdb_id = tmdb_movie.get('id')
                if tmdb_id:
                    details = self.get_movie_details(tmdb_id)
                    film_data = self._extract_film_data(details, details)
            
            # Store financial data per film (use normalized name as key)
            if film_data['budget'] > 0:
                self.stats_aggregator['film_budgets'][name] = film_data['budget']
            if film_data['box_office'] > 0:
                self.stats_aggregator['film_boxoffices'][name] = film_data['box_office']
            
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
        
        # Print TMDB match statistics summary
        ms = self.match_stats
        if ms['total'] > 0:
            avg_year_delta = sum(ms['year_deltas']) / len(ms['year_deltas']) if ms['year_deltas'] else 0
            avg_confidence = sum(ms['confidences']) / len(ms['confidences']) if ms['confidences'] else 0
            fast_pct = (ms['fast_path'] / ms['total']) * 100
            fallback_pct = (ms['fallback_path'] / ms['total']) * 100
            match_rate = ((ms['total'] - ms['no_match']) / ms['total']) * 100
            
            print("\n" + "=" * 50)
            print("TMDB MATCH STATISTICS")
            print("=" * 50)
            print(f"  Total films: {ms['total']}")
            print(f"  Match rate: {match_rate:.1f}% ({ms['total'] - ms['no_match']}/{ms['total']})")
            print(f"  Fast path (STEP 1): {ms['fast_path']} ({fast_pct:.1f}%)")
            print(f"  Fallback path (STEP 2): {ms['fallback_path']} ({fallback_pct:.1f}%)")
            print(f"  No match found: {ms['no_match']}")
            print(f"  Low confidence (year delta > 5): {ms['low_confidence']}")
            print(f"  Average year delta: {avg_year_delta:.1f} years")
            print(f"  Average confidence: {avg_confidence:.2f}")
            print("=" * 50)
        
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
        
        # Clear financial data
        stats.film_budget_data.clear()
        stats.film_boxoffice_data.clear()
        
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
        
        # Transfer financial data
        for k, v in self.stats_aggregator['film_budgets'].items():
            stats.film_budget_data[k] = int(v)
        for k, v in self.stats_aggregator['film_boxoffices'].items():
            stats.film_boxoffice_data[k] = int(v)
    
    def scrape_user_profile(self, csv_path):
        """Synchronous entry point - compatible with existing scraper interface."""
        self.scrape_csv_file(csv_path)
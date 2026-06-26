"""
Folder-based import system.
Replaces single-file CSV import with a structured folder-based ingestion method.

Expected folder structure:
    folder/
    ├── watched.csv      (REQUIRED - used for main statistics)
    ├── diary.csv        (REQUIRED - parsed separately, reserved for future display)
    └── watchlist.csv    (OPTIONAL - future use, silently ignored if missing)

All other CSV files in the folder are silently ignored.
"""
import csv
import logging
import os
from pathlib import Path
from typing import Optional, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)


class ImportValidationError(Exception):
    """Raised when folder validation fails."""
    pass


class FolderImportValidator:
    """Validates folder structure and required CSV files."""

    REQUIRED_FILES = ['watched.csv', 'diary.csv']
    SUPPORTED_FILES = ['watched.csv', 'diary.csv', 'watchlist.csv']

    def validate_folder(self, folder_path: str) -> list[str]:
        """Validate that the folder exists and contains all required files.

        Args:
            folder_path: Path to the folder to validate.

        Returns:
            List of found supported CSV files.

        Raises:
            ImportValidationError: If validation fails.
        """
        folder = Path(folder_path)

        # Check folder exists
        if not folder.exists():
            raise ImportValidationError(f"Folder does not exist: {folder_path}")

        if not folder.is_dir():
            raise ImportValidationError(f"Path is not a directory: {folder_path}")

        # Check for required files
        found_files = []
        missing_files = []

        for filename in self.SUPPORTED_FILES:
            file_path = folder / filename
            if file_path.exists() and file_path.is_file():
                found_files.append(filename)
            elif filename in self.REQUIRED_FILES:
                missing_files.append(filename)

        # Check for any CSV files at all
        all_csv = list(folder.glob('*.csv'))
        if not all_csv and not missing_files:
            raise ImportValidationError(f"Folder is empty - no CSV files found: {folder_path}")

        # Check for required files
        if missing_files:
            raise ImportValidationError(
                f"Missing required files: {', '.join(missing_files)} in {folder_path}"
            )

        logger.info(f"Folder validation passed: {found_files} found in {folder_path}")
        return found_files

    def get_supported_files(self, folder_path: str) -> list[str]:
        """Get list of supported CSV files present in the folder.

        Args:
            folder_path: Path to the folder.

        Returns:
            List of supported CSV filenames that exist in the folder.
        """
        folder = Path(folder_path)
        return [f for f in self.SUPPORTED_FILES if (folder / f).exists()]


class CSVFileParser:
    """Parses Letterboxd-exported CSV files.

    Supports formats:
    - watched.csv / watchlist.csv: Date, Name, Year, Letterboxd URI
    - diary.csv: Date, Name, Year, Letterboxd URI (diary entries)

    Returns a list of (date, name, year, uri) tuples.
    """

    # Column name mappings for detection
    NAME_COLUMNS = {'name', 'title', 'film'}
    YEAR_COLUMNS = {'year', 'release_year'}
    DATE_COLUMNS = {'date'}
    WATCHED_DATE_COLUMNS = {'watched date'}
    URI_COLUMNS = {'letterboxd uri', 'uri', 'link'}

    def parse_csv_file(self, csv_path: str) -> list[tuple[str, str, str, str, str]]:
        """Parse a Letterboxd-exported CSV file.

        Returns 5-tuples: (date, watched_date, name, year, uri).
        For watched.csv / watchlist.csv, watched_date will be empty string.
        For diary.csv, watched_date contains the actual "Watched Date" value.

        Args:
            csv_path: Path to the CSV file.

        Returns:
            List of (date, watched_date, name, year, uri) tuples.
        """
        entries = []

        try:
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                # Detect delimiter
                sample = f.read(4096)
                f.seek(0)

                try:
                    dialect = csv.Sniffer().sniff(sample, delimiters=',;\t|')
                    delimiter = dialect.delimiter
                except csv.Error:
                    delimiter = ','

                reader = csv.reader(f, delimiter=delimiter)

                # Read header
                try:
                    header = [h.strip().lower() for h in next(reader)]
                except StopIteration:
                    logger.warning(f"Empty CSV file: {csv_path}")
                    return []

                # Find column indices
                name_idx = self._find_column(header, self.NAME_COLUMNS)
                year_idx = self._find_column(header, self.YEAR_COLUMNS)
                date_idx = self._find_column(header, self.DATE_COLUMNS)
                watched_date_idx = self._find_column(header, self.WATCHED_DATE_COLUMNS)
                uri_idx = self._find_column(header, self.URI_COLUMNS)

                # Validate name column exists
                if name_idx is None:
                    logger.warning(f"Could not find 'Name' column in CSV headers: {header}")
                    return []

                # Parse rows
                for row_num, row in enumerate(reader, start=2):
                    if not row or all(cell.strip() == '' for cell in row):
                        continue

                    name = row[name_idx].strip() if name_idx < len(row) else ''
                    if not name:
                        continue

                    year = ''
                    if year_idx is not None and year_idx < len(row):
                        year = row[year_idx].strip()

                    date = ''
                    if date_idx is not None and date_idx < len(row):
                        date = row[date_idx].strip()

                    watched_date = ''
                    if watched_date_idx is not None and watched_date_idx < len(row):
                        watched_date = row[watched_date_idx].strip()

                    uri = ''
                    if uri_idx is not None and uri_idx < len(row):
                        uri = row[uri_idx].strip()

                    entries.append((date, watched_date, name, year, uri))

        except FileNotFoundError:
            logger.error(f"CSV file not found: {csv_path}")
        except Exception as e:
            logger.error(f"Error parsing CSV file {csv_path}: {e}")

        return entries

    def _find_column(self, header: list[str], candidates: set[str]) -> Optional[int]:
        """Find the index of a column matching any of the candidate names.

        Args:
            header: CSV header row.
            candidates: Set of column names to look for.

        Returns:
            Column index or None if not found.
        """
        for i, col in enumerate(header):
            if col in candidates:
                return i
        return None

    def extract_unique_films(self, entries: list[tuple[str, str, str, str, str]]) -> list[tuple[str, str]]:
        """Extract unique films from parsed entries.

        Deduplicates by (name, year) combination, preserving order of first occurrence.

        Args:
            entries: List of (date, watched_date, name, year, uri) tuples.

        Returns:
            List of unique (name, year) tuples.
        """
        seen = set()
        unique_films = []

        for date, watched_date, name, year, uri in entries:
            key = (name.lower().strip(), year.strip())
            if key not in seen:
                seen.add(key)
                unique_films.append((name, year))

        logger.debug(f"Extracted {len(unique_films)} unique films from {len(entries)} entries")
        return unique_films


class FolderDataLoader:
    """Loads and organizes data from a validated folder structure."""

    def __init__(self):
        self.validator = FolderImportValidator()
        self.parser = CSVFileParser()

    def load_folder(self, folder_path: str) -> dict:
        """Load all supported CSV files from the folder.

        Args:
            folder_path: Path to the validated folder.

        Returns:
            Dictionary with structure:
            {
                'watched': [(date, name, year, uri), ...],
                'diary': [(date, name, year, uri), ...],
                'watchlist': [(date, name, year, uri), ...] or None,
                'found_files': ['watched.csv', 'diary.csv', ...]
            }
        """
        # Validate folder
        found_files = self.validator.validate_folder(folder_path)

        result = {
            'watched': [],
            'diary': [],
            'watchlist': None,
            'found_files': found_files,
            'folder_path': folder_path
        }

        # Parse watched.csv (always present - required)
        watched_path = Path(folder_path) / 'watched.csv'
        if 'watched.csv' in found_files:
            result['watched'] = self.parser.parse_csv_file(str(watched_path))
            logger.info(f"Loaded {len(result['watched'])} entries from watched.csv")

        # Parse diary.csv (always present - required)
        diary_path = Path(folder_path) / 'diary.csv'
        if 'diary.csv' in found_files:
            result['diary'] = self.parser.parse_csv_file(str(diary_path))
            logger.info(f"Loaded {len(result['diary'])} entries from diary.csv")

        # Parse watchlist.csv (optional)
        watchlist_path = Path(folder_path) / 'watchlist.csv'
        if 'watchlist.csv' in found_files:
            result['watchlist'] = self.parser.parse_csv_file(str(watchlist_path))
            logger.info(f"Loaded {len(result['watchlist'])} entries from watchlist.csv")

        return result


class FolderScraperCoordinator:
    """Coordinates scraping for folder-based import.

    Processes watched.csv and diary.csv separately, maintaining distinct
    data pools for each source.
    """

    def __init__(self, scraper, app_context):
        """Initialize the coordinator.

        Args:
            scraper: TMDbScraper instance.
            app_context: AppContext instance.
        """
        self.scraper = scraper
        self.app_context = app_context
        self.data_loader = FolderDataLoader()

        # Separate aggregation for watched vs diary
        self.watched_stats = {
            'languages': {},
            'countries': {},
            'genres': {},
            'directors': {},
            'actors': {},
            'decades': {},
            'runtimes': []
        }
        self.diary_stats = {
            'languages': {},
            'countries': {},
            'genres': {},
            'directors': {},
            'actors': {},
            'decades': {},
            'runtimes': []
        }

        # Deduplication cache: stores (name.lower().strip(), year.strip()) of already-scraped movies
        self._scraped_cache = {}
        
        # Film records storage: stores complete film data for export
        # Key format: "title|year" (normalized)
        self._film_records: Dict[str, Dict[str, Any]] = {}
        
        # Diary entries storage: stores diary-specific metadata
        # Each entry: {date, title, year, rating, tags, notes, tmdb_id}
        self._diary_entries: list = []
        
        # Watched entries storage
        # Each entry: {title, year, date}
        self._watched_entries: list = []
        
        # Raw CSV content storage
        self._raw_watched_csv: str = ""
        self._raw_diary_csv: str = ""
        self._raw_watchlist_csv: str = ""
        
        # Watchlist data storage
        self._watchlist_data: list = []
    
    def set_raw_csv_content(self, watched: str = "", diary: str = "", watchlist: str = ""):
        """Set raw CSV content for snapshot preservation.
        
        Args:
            watched: Raw watched.csv content
            diary: Raw diary.csv content
            watchlist: Raw watchlist.csv content
        """
        self._raw_watched_csv = watched
        self._raw_diary_csv = diary
        self._raw_watchlist_csv = watchlist
    
    def get_film_records(self) -> Dict[str, Dict[str, Any]]:
        """Get all stored film records.
        
        Returns:
            Dictionary keyed by "title|year" with film data values.
        """
        return dict(self._film_records)
    
    def get_diary_entries(self) -> list:
        """Get all stored diary entries.
        
        Returns:
            List of diary entry dicts.
        """
        return list(self._diary_entries)
    
    def get_watched_entries(self) -> list:
        """Get all stored watched entries.
        
        Returns:
            List of watched entry dicts.
        """
        return list(self._watched_entries)
    
    def get_raw_csv_content(self) -> tuple:
        """Get raw CSV content.
        
        Returns:
            Tuple of (watched_csv, diary_csv, watchlist_csv) strings.
        """
        return (self._raw_watched_csv, self._raw_diary_csv, self._raw_watchlist_csv)

    def _load_raw_csv_content(self, folder_path: str):
        """Load raw CSV content from the folder for snapshot preservation.
        
        Args:
            folder_path: Path to the folder containing CSV files.
        """
        folder = Path(folder_path)
        
        # Load watched.csv
        watched_path = folder / 'watched.csv'
        if watched_path.exists():
            try:
                with open(watched_path, 'r', encoding='utf-8-sig') as f:
                    self._raw_watched_csv = f.read()
            except IOError as e:
                logger.warning(f"Failed to read raw watched.csv: {e}")
        
        # Load diary.csv
        diary_path = folder / 'diary.csv'
        if diary_path.exists():
            try:
                with open(diary_path, 'r', encoding='utf-8-sig') as f:
                    self._raw_diary_csv = f.read()
            except IOError as e:
                logger.warning(f"Failed to read raw diary.csv: {e}")
        
        # Load watchlist.csv if present
        watchlist_path = folder / 'watchlist.csv'
        if watchlist_path.exists():
            try:
                with open(watchlist_path, 'r', encoding='utf-8-sig') as f:
                    self._raw_watchlist_csv = f.read()
            except IOError as e:
                logger.warning(f"Failed to read raw watchlist.csv: {e}")
    
    def scrape_folder(self, folder_path: str, progress_callback=None) -> dict:
        """Scrape all films from a validated folder.

        Processes watched.csv first, then diary.csv separately.

        Args:
            folder_path: Path to the validated folder.
            progress_callback: Optional callback(percent, status) for progress updates.

        Returns:
            Dictionary with:
            {
                'success': True,
                'username': str,
                'films_count': int,
                'total_hours': float,
                'total_days': float,
                'scraped_at': str,
                'countries': dict,
                'languages': dict,
                'genres': dict,
                'directors': dict,
                'actors': dict,
                'decades': dict,
                'watched_films_count': int,
                'diary_films_count': int,
                'watched_stats': {...},
                'diary_stats': {...}
            }
        """
        import time as time_module

        # Set progress callback
        if progress_callback:
            self.scraper.progress_callback = progress_callback

        # Load folder data
        logger.info(f"Loading folder: {folder_path}")
        folder_data = self.data_loader.load_folder(folder_path)
        
        # Load raw CSV content for snapshot preservation
        self._load_raw_csv_content(folder_path)

        # Reset data
        self.app_context.stats_data.reset()
        self.app_context.gui_models.clear_all()

        # Reset aggregation
        self._reset_aggregation()

        # Reset deduplication cache
        self._scraped_cache = {}
        
        # Reset film records and entries storage
        self._film_records = {}
        self._diary_entries = []
        self._watched_entries = []

        # Step 1: Process diary.csv FIRST (contains all films + date watched metadata)
        diary_entries = folder_data['diary']
        diary_films = self.data_loader.parser.extract_unique_films(diary_entries)
        logger.info(f"Step 1: Processing {len(diary_films)} unique films from diary.csv ({len(diary_entries)} total entries, {len(diary_entries) - len(diary_films)} duplicates for scraping)")

        if progress_callback:
            progress_callback(5, f"Processing diary entries ({len(diary_films)} films)...")

        diary_start_time = time_module.time()
        self._process_films(diary_films, 'diary', start_time=diary_start_time)
        diary_count = len(diary_films)

        # Step 2: Process watched.csv - skip films already in diary cache
        watched_entries = folder_data['watched']
        watched_films = self.data_loader.parser.extract_unique_films(watched_entries)
        logger.info(f"Step 2: Checking {len(watched_films)} unique films from watched.csv ({len(watched_entries)} total entries) against diary cache...")

        # Filter watched films: only process those NOT already in diary cache
        watched_new_films = []
        watched_cached_count = 0
        for name, year in watched_films:
            film_key = (name.lower().strip(), year.strip())
            if film_key in self._scraped_cache:
                watched_cached_count += 1
            else:
                watched_new_films.append((name, year))

        logger.info(f"Watched: {watched_cached_count} films already in diary cache (skipped), {len(watched_new_films)} new films to scrape")

        watched_count = len(watched_films)

        if progress_callback:
            if len(watched_new_films) > 0:
                progress_callback(45, f"Processing {len(watched_new_films)} additional watched films...")
            else:
                progress_callback(45, "All watched films already in diary - skipping...")

        if len(watched_new_films) > 0:
            watched_batch_start = time_module.time()
            self._process_films(watched_new_films, 'watched', start_time=watched_batch_start)

        # Store diary entries from diary.csv data
        self._store_diary_entries(diary_entries)
        logger.info(f"Stored {len(self._diary_entries)} diary entries for analytics (all entries including duplicates)")
        
        # Store watched entries from watched.csv data
        self._store_watched_entries(watched_entries)
        logger.info(f"Stored {len(self._watched_entries)} watched entries")
        
        # Store watchlist data from watchlist.csv if available
        if folder_data['watchlist']:
            self._store_watchlist_data(folder_data['watchlist'])

        # Transfer aggregated data to app context
        self._transfer_aggregated_data()

        # Compute diary analytics (weekday/month/year aggregation) + financial analytics
        self._compute_diary_and_financial_analytics()

        # Generate GUI strings - only use watched count for total
        total_films = watched_count  # Only watched films count for analytics
        self.app_context.stats_data.set_meta_data(
            total_films,
            self.app_context.stats_data.total_hours,
            self.app_context.stats_data.total_days,
            __import__('time').strftime("%d/%m/%Y", __import__('time').localtime())
        )

        # Build result
        scraped_when = __import__('time').strftime("%d/%m/%Y", __import__('time').localtime())
        stats = self.app_context.stats_data

        result = {
            'success': True,
            'username': 'letterboxd_export',
            'films_count': total_films,
            'total_hours': stats.total_hours,
            'total_days': stats.total_days,
            'scraped_at': scraped_when,
            'countries': self._dict_to_json(stats.country_dict),
            'languages': self._dict_to_json(stats.lang_dict),
            'genres': self._dict_to_json(stats.genre_dict),
            'directors': self._dict_to_json(stats.director_dict),
            'actors': self._dict_to_json(stats.actor_dict),
            'decades': self._dict_to_json(dict(stats.decade_dict)),
            'watched_films_count': watched_count,
            'diary_films_count': diary_count,
            'folder_path': folder_path
        }

        logger.info(f"Folder scraping complete: {total_films} watched films, {diary_count} diary films (analytics use ALL {len(self._diary_entries)} diary entries)")
        return result

    def _reset_aggregation(self):
        """Reset all aggregation dictionaries."""
        for key in self.watched_stats:
            if key == 'runtimes':
                self.watched_stats[key] = []
            else:
                self.watched_stats[key] = {}

        for key in self.diary_stats:
            if key == 'runtimes':
                self.diary_stats[key] = []
            else:
                self.diary_stats[key] = {}

    def _process_films(self, films: list[tuple[str, str]], source: str, start_time: float = None):
        """Process a list of films through the TMDB scraper.

        Uses isolated batch counters, deduplication cache, and full progress metrics.

        Args:
            films: List of (name, year) tuples.
            source: Either 'watched' or 'diary'.
            start_time: Epoch time when this batch started (for ETA calculation).
        """
        import time as time_module

        stats_dict = self.watched_stats if source == 'watched' else self.diary_stats
        batch_start = start_time or time_module.time()

        # Local batch counter (isolated per batch, not shared global counter)
        batch_processed = 0
        total = len(films)

        for i, (name, year) in enumerate(films):
            # --- Fix 3: Deduplication check ---
            film_key = (name.lower().strip(), year.strip())

            if film_key in self._scraped_cache:
                # Already scraped this movie - reuse cached data
                film_data = self._scraped_cache[film_key]
                logger.debug(f"Skipping duplicate: {name} ({year}) - using cached data")
            else:
                # Fresh scrape
                tmdb_movie = self.scraper.search_movie(name, year if year else None)

                film_data = {'languages': [], 'countries': [], 'genres': [],
                            'directors': [], 'actors': [], 'decade': None, 'runtime': 0}

                if tmdb_movie:
                    tmdb_id = tmdb_movie.get('id')
                    if tmdb_id:
                        details = self.scraper.get_movie_details(tmdb_id)
                        film_data = self.scraper._extract_film_data(details, details)

                # Cache the result
                self._scraped_cache[film_key] = film_data

            # Aggregate data into the appropriate source pool
            for lang in film_data['languages']:
                stats_dict['languages'][lang] = stats_dict['languages'].get(lang, 0) + 1

            for country in film_data['countries']:
                stats_dict['countries'][country] = stats_dict['countries'].get(country, 0) + 1

            for genre in film_data['genres']:
                stats_dict['genres'][genre] = stats_dict['genres'].get(genre, 0) + 1

            for director in film_data['directors']:
                stats_dict['directors'][director] = stats_dict['directors'].get(director, 0) + 1

            for actor in film_data['actors']:
                stats_dict['actors'][actor] = stats_dict['actors'].get(actor, 0) + 1

            if film_data['decade']:
                stats_dict['decades'][film_data['decade']] = stats_dict['decades'].get(film_data['decade'], 0) + 1

            if film_data['runtime'] > 0:
                stats_dict['runtimes'].append(film_data['runtime'])

            # Store film record for snapshot export
            film_key = f"{name}|{year}"
            if film_key not in self._film_records:
                self._film_records[film_key] = {
                    'title': name,
                    'year': year,
                    'tmdb_id': film_data.get('tmdb_id'),
                    'languages': film_data.get('languages', []),
                    'countries': film_data.get('countries', []),
                    'genres': film_data.get('genres', []),
                    'directors': film_data.get('directors', []),
                    'actors': film_data.get('actors', []),
                    'decade': film_data.get('decade'),
                    'runtime': film_data.get('runtime', 0),
                    'budget': film_data.get('budget'),
                    'box_office': film_data.get('box_office'),
                    'overview': film_data.get('overview', ''),
                    'poster_path': film_data.get('poster_path', ''),
                    'in_watchlist': False,
                    'added_to_watchlist_date': None,
                    'times_watched': 0,
                    'average_rating': None
                }
            else:
                # Update existing record with additional data
                existing = self._film_records[film_key]
                # Merge languages
                for lang in film_data.get('languages', []):
                    if lang not in existing['languages']:
                        existing['languages'].append(lang)
                # Merge countries
                for country in film_data.get('countries', []):
                    if country not in existing['countries']:
                        existing['countries'].append(country)
                # Merge genres
                for genre in film_data.get('genres', []):
                    if genre not in existing['genres']:
                        existing['genres'].append(genre)
                # Merge directors
                for director in film_data.get('directors', []):
                    if director not in existing['directors']:
                        existing['directors'].append(director)
                # Merge actors
                for actor in film_data.get('actors', []):
                    if actor not in existing['actors']:
                        existing['actors'].append(actor)
                # Update tmdb_id if not set
                if not existing['tmdb_id'] and film_data.get('tmdb_id'):
                    existing['tmdb_id'] = film_data.get('tmdb_id')
                # Update runtime if better data
                if film_data.get('runtime', 0) > existing['runtime']:
                    existing['runtime'] = film_data.get('runtime', 0)
                # Update budget/box_office if available
                if film_data.get('budget') is not None and existing['budget'] is None:
                    existing['budget'] = film_data.get('budget')
                if film_data.get('box_office') is not None and existing['box_office'] is None:
                    existing['box_office'] = film_data.get('box_office')

            # --- Fix 1 & 2: Isolated progress with full metrics ---
            batch_processed += 1
            if total > 0 and self.scraper.progress_callback:
                # Progress is relative to current batch only (0-100% range per batch)
                progress = int(100 * batch_processed / total)

                # Calculate speed and ETA from batch start time
                elapsed_time = time_module.time() - batch_start
                remaining = total - batch_processed

                if batch_processed > 0 and elapsed_time > 0:
                    speed = batch_processed / elapsed_time
                    eta_seconds = remaining / speed if speed > 0 else 0

                    # Format ETA string
                    if eta_seconds < 60:
                        eta_str = f"{int(eta_seconds)}s"
                    else:
                        minutes = int(eta_seconds // 60)
                        seconds = int(eta_seconds % 60)
                        eta_str = f"{minutes}m{seconds}s"

                    status = f"Processing {source} films ({batch_processed}/{total}) | {speed:.1f} films/s | ETA: {eta_str}"
                else:
                    status = f"Processing {source} films ({batch_processed}/{total})"

                self.scraper.progress_callback(progress, status)

                # Push detailed stats to frontend for progress bar display
                if hasattr(self.scraper.progress_callback, '__self__'):
                    api = self.scraper.progress_callback.__self__
                    if hasattr(api, 'set_progress_stats'):
                        if batch_processed > 0 and elapsed_time > 0:
                            api.set_progress_stats(batch_processed, total, speed, eta_seconds)

            # Rate limiting
            if i % 10 == 9:
                import time
                time.sleep(self.scraper.batch_delay)

    def _transfer_aggregated_data(self):
        """Transfer aggregated data to app context statistics.

        Merges BOTH watched and diary stats into main stats_data.
        diary_stats is kept internally for FUTURE USE (separate display).
        """
        stats = self.app_context.stats_data

        # Clear existing data
        stats.lang_dict.clear()
        stats.country_dict.clear()
        stats.genre_dict.clear()
        stats.director_dict.clear()
        stats.actor_dict.clear()
        stats.decade_dict.clear()

        # Merge BOTH watched and diary stats (diary contains all films + watched is subset)
        watched = self.watched_stats
        diary = self.diary_stats

        # Merge languages (watched + diary unique films)
        all_langs = {}
        for k, v in watched['languages'].items():
            all_langs[k] = v
        for k, v in diary['languages'].items():
            all_langs[k] = all_langs.get(k, 0) + v
        stats.lang_dict.update(all_langs)

        # Merge countries
        all_countries = {}
        for k, v in watched['countries'].items():
            all_countries[k] = v
        for k, v in diary['countries'].items():
            all_countries[k] = all_countries.get(k, 0) + v
        stats.country_dict.update(all_countries)

        # Merge genres
        all_genres = {}
        for k, v in watched['genres'].items():
            all_genres[k] = v
        for k, v in diary['genres'].items():
            all_genres[k] = all_genres.get(k, 0) + v
        stats.genre_dict.update(all_genres)

        # Merge directors
        all_directors = {}
        for k, v in watched['directors'].items():
            all_directors[k] = v
        for k, v in diary['directors'].items():
            all_directors[k] = all_directors.get(k, 0) + v
        stats.director_dict.update(all_directors)

        # Merge actors
        all_actors = {}
        for k, v in watched['actors'].items():
            all_actors[k] = v
        for k, v in diary['actors'].items():
            all_actors[k] = all_actors.get(k, 0) + v
        stats.actor_dict.update(all_actors)

        # Merge decades
        all_decades = {}
        for k, v in watched['decades'].items():
            all_decades[k] = v
        for k, v in diary['decades'].items():
            all_decades[k] = all_decades.get(k, 0) + v
        stats.decade_dict.update(all_decades)

        # Merge runtimes for total hours/days
        all_runtimes = watched['runtimes'] + diary['runtimes']
        hrs = sum(all_runtimes) / 60 if all_runtimes else 0.0
        dys = hrs / 24
        stats.total_hours = hrs
        stats.total_days = dys

    def _compute_diary_and_financial_analytics(self):
        """Compute diary analytics (weekday/month/year) and financial analytics (budget/box-office ranking).
        
        Uses the stored diary entries and film records to compute:
        - diary_weekday_counts: entries grouped by weekday name
        - diary_month_counts: entries grouped by YYYY-MM
        - diary_year_counts: entries grouped by YYYY
        - film_budget_data: per-film budget values for ranking
        - film_boxoffice_data: per-film box office values for ranking
        """
        stats = self.app_context.stats_data
        
        logger.info(f"Computing diary analytics from {len(self._diary_entries)} entries")
        logger.info(f"Computing financial analytics from {len(self._film_records)} unique films")
        
        # --- Diary analytics ---
        weekday_counts = {}
        month_counts = {}
        year_counts = {}
        
        for entry in self._diary_entries:
            date_str = entry.get('date', '')
            if not date_str:
                continue
            
            # Parse date - expect YYYY-MM-DD format
            try:
                parts = date_str.split('-')
                if len(parts) >= 3:
                    year_str = parts[0]
                    month_str = parts[1]
                    
                    # Compute weekday name
                    import datetime
                    date_obj = datetime.datetime(int(year_str), int(month_str), int(parts[2]))
                    weekday_name = date_obj.strftime('%A')
                    
                    # Count by weekday
                    weekday_counts[weekday_name] = weekday_counts.get(weekday_name, 0) + 1
                    
                    # Count by month (YYYY-MM)
                    month_key = f"{year_str}-{month_str}"
                    month_counts[month_key] = month_counts.get(month_key, 0) + 1
                    
                    # Count by year (YYYY)
                    year_counts[year_str] = year_counts.get(year_str, 0) + 1
            except (ValueError, IndexError):
                # Skip invalid dates
                continue
        
        # Store in stats_data
        stats.diary_weekday_counts = weekday_counts
        stats.diary_month_counts = month_counts
        stats.diary_year_counts = year_counts
        
        # --- Financial analytics ---
        # Extract per-film budget and box_office data from film records
        budget_data = {}
        boxoffice_data = {}
        
        for film_key, film_data in self._film_records.items():
            title = film_data.get('title', '')
            budget = film_data.get('budget')
            box_office = film_data.get('box_office')
            
            if budget is not None and budget > 0:
                budget_data[title] = budget
            
            if box_office is not None and box_office > 0:
                boxoffice_data[title] = box_office
        
        stats.film_budget_data = budget_data
        stats.film_boxoffice_data = boxoffice_data

    def _store_diary_entries(self, diary_entries: list):
        """Store diary entries with metadata from diary.csv data.
        
        Uses Watched Date (not log Date) for all diary analytics aggregation.
        
        Args:
            diary_entries: List of (date, watched_date, name, year, uri) tuples from diary.csv
        """
        for date, watched_date, name, year, uri in diary_entries:
            # Try to find matching film record for additional metadata
            film_key = f"{name}|{year}"
            tmdb_id = None
            if film_key in self._film_records:
                tmdb_id = self._film_records[film_key].get('tmdb_id')
            
            # Use Watched Date as the primary date for analytics
            # For snapshot export, store both: 'date' = watched_date (analytics), 'log_date' = original date
            entry = {
                'date': watched_date,  # Analytics date = Watched Date (FIXED)
                'log_date': date,      # Original log date (metadata only)
                'title': name,
                'year': year,
                'rating': None,  # Will be populated from ratings.csv if available
                'tags': [],
                'notes': '',
                'tmdb_id': tmdb_id
            }
            self._diary_entries.append(entry)
    
    def _store_watched_entries(self, watched_entries: list):
        """Store watched entries from watched.csv data.
        
        Args:
            watched_entries: List of (date, watched_date, name, year, uri) tuples from watched.csv
        """
        for date, watched_date, name, year, uri in watched_entries:
            entry = {
                'title': name,
                'year': year,
                'date': date
            }
            self._watched_entries.append(entry)
    
    def _store_watchlist_data(self, watchlist_entries: list):
        """Store watchlist data from watchlist.csv if available.
        
        Args:
            watchlist_entries: List of (date, watched_date, name, year, uri) tuples from watchlist.csv
        """
        if not watchlist_entries:
            return
        
        self._watchlist_data = []
        for date, watched_date, name, year, uri in watchlist_entries:
            entry = {
                'title': name,
                'year': year,
                'date': date,
                'uri': uri
            }
            self._watchlist_data.append(entry)
            
            # Update film record with watchlist status
            film_key = f"{name}|{year}"
            if film_key in self._film_records:
                self._film_records[film_key]['in_watchlist'] = True
                self._film_records[film_key]['added_to_watchlist_date'] = date
    
    @property
    def parser(self):
        """Get the CSV file parser."""
        return self.data_loader.parser

    @staticmethod
    def _dict_to_json(data_dict: dict) -> str:
        """Convert a dictionary to JSON string.

        Args:
            data_dict: Dictionary to convert.

        Returns:
            JSON string representation.
        """
        import json
        return json.dumps(data_dict)
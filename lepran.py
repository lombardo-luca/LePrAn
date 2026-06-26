"""
LePrAn - Letterboxd Profile Analyzer
Web-based GUI using pywebview.
Main application entry point.
"""
import os
import sys
import json
import logging
import threading
from pathlib import Path

import webview
import colorama
colorama.init()

# Add project root to path for imports
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from src.context import AppContext
from src.scraper_tmdb import TMDbScraper
from src.data_manager import DataManager
from src.folder_import import FolderScraperCoordinator, ImportValidationError
from src.snapshot import ApplicationSnapshot
from src.snapshot_export import SnapshotExporter
from src.snapshot_import import SnapshotImporter, SnapshotImportError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Suppress urllib3 warnings
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)


class WebAPI:
    """Python API exposed to the webview JavaScript bridge."""
    
    def __init__(self, app_context, window=None):
        self.app_context = app_context
        self.data_manager = DataManager(app_context)
        self.scraper = TMDbScraper(app_context)
        self.login_input = "analysis"
        
        # Settings window reference
        self.settings_window = None
        
        # pywebview main window for evaluate_js (callback push)
        self._window = window
        
        # Progress tracking for async analysis
        self._analysis_progress = 0
        self._analysis_status = ''
        self._analysis_running = False
        self._analysis_result = None
        self._analysis_error = None
        
        # Detailed progress stats
        self._films_processed = 0
        self._films_total = 0
        self._films_speed = 0.0
        self._eta_seconds = 0.0
    
    @staticmethod
    def _read_username_from_profile_csv(folder_path: str) -> str:
        """Read the username from profile.csv in the given folder.
        
        Letterboxd exports include a profile.csv file with user metadata.
        The 'Username' field contains the display name to show in the UI.
        
        Args:
            folder_path: Path to the Letterboxd export folder.
            
        Returns:
            The username string if found, empty string otherwise.
        """
        import csv
        profile_path = os.path.join(folder_path, 'profile.csv')
        
        try:
            with open(profile_path, 'r', encoding='utf-8-sig') as f:
                # Detect delimiter
                sample = f.read(4096)
                f.seek(0)
                
                try:
                    dialect = csv.Sniffer().sniff(sample, delimiters=',;\t|')
                    delimiter = dialect.delimiter
                except csv.Error:
                    delimiter = ','
                
                reader = csv.DictReader(f, delimiter=delimiter)
                
                # Look for 'Username' field (case-insensitive matching)
                for row in reader:
                    # Find the username key (could be 'Username', 'username', 'NAME', etc.)
                    for key in row:
                        if key.strip().lower() == 'username':
                            value = row[key].strip()
                            if value:
                                return value
        except FileNotFoundError:
            logger.debug(f"profile.csv not found in {folder_path}")
        except Exception as e:
            logger.warning(f"Failed to read profile.csv from {folder_path}: {e}")
        
        return ''
    
    def _update_progress(self, percent, status):
        """Update analysis progress (called from scraper callback).
        
        Pushes progress update directly to the frontend via evaluate_js.
        Uses main_thread to ensure thread-safe execution on UI thread.
        """
        self._analysis_progress = percent
        self._analysis_status = status
        
        # Escape special characters in status message for safe JS injection
        safe_status = status.replace('\\', '\\\\').replace("'", "\\'").replace('\n', ' ').replace('\r', '')
        
        if self._window:
            try:
                from webview.util import main_thread
                main_thread(
                    lambda: self._window.evaluate_js(
                        f'window.__lepranProgress({percent}, "{safe_status}", {self._films_processed}, {self._films_total}, {self._films_speed:.1f}, {self._eta_seconds:.0f})'
                    )
                )
            except Exception as e:
                logger.debug(f"Failed to push progress update: {e}")
    
    def set_progress_stats(self, films_processed, films_total, speed, eta_seconds):
        """Set detailed progress statistics from the analysis thread."""
        self._films_processed = films_processed
        self._films_total = films_total
        self._films_speed = speed
        self._eta_seconds = eta_seconds
    
    def select_folder(self):
        """Open a folder selection dialog and return the selected path.
        
        Returns:
            dict with 'folder_path' on success, or 'error' on failure.
        """
        try:
            import tkinter as tk
            from tkinter import filedialog
            
            root = tk.Tk()
            root.withdraw()
            folder_path = filedialog.askdirectory(
                title="Select Letterboxd Export Folder",
                initialdir=os.path.abspath('.')
            )
            root.destroy()
            
            logger.info(f"select_folder: selected folder_path={folder_path}")
            
            if not folder_path:
                return {'success': False, 'error': 'No folder selected'}
            
            return {'success': True, 'folder_path': folder_path}
            
        except Exception as e:
            logger.error(f"Error selecting folder: {e}")
            return {'success': False, 'error': str(e)}
    
    def analyze_folder(self, folder_path):
        """Analyze a folder containing Letterboxd export CSV files.
        
        Expected folder structure:
            folder/
            ├── watched.csv      (REQUIRED)
            ├── diary.csv        (REQUIRED)
            └── watchlist.csv    (OPTIONAL)
        
        Starts analysis in a background thread and returns immediately.
        Progress can be polled via get_analysis_progress().
        When complete, the result is available via get_analysis_progress()['result'].
        """
        try:
            # Reset progress state
            self._analysis_progress = 0
            self._analysis_status = 'Validating folder...'
            self._analysis_running = True
            self._analysis_result = None
            self._analysis_error = None
            
            # Create scraper with progress callback
            scraper_with_callback = TMDbScraper(self.app_context, progress_callback=self._update_progress)
            
            def run_analysis():
                try:
                    coordinator = FolderScraperCoordinator(scraper_with_callback, self.app_context)
                    coordinator.scrape_folder(folder_path, progress_callback=self._update_progress)
                    
                    # Store coordinator reference for snapshot export
                    self._coordinator = coordinator
                    
                    # Read username from profile.csv (authoritative source)
                    profile_username = WebAPI._read_username_from_profile_csv(folder_path)
                    if profile_username:
                        self.login_input = profile_username
                        logger.info(f"Username loaded from profile.csv: {profile_username}")
                    
                    self.data_manager.generate_gui_strings(self.app_context.stats_data.films_count)
                    self._analysis_result = self._build_result()
                except ImportValidationError as e:
                    logger.error(f"Folder validation error: {e}")
                    self._analysis_error = str(e)
                except Exception as e:
                    logger.error(f"Error analyzing folder: {e}")
                    self._analysis_error = str(e)
                finally:
                    self._analysis_running = False
            
            # Start analysis in background thread - return immediately
            thread = threading.Thread(target=run_analysis, daemon=True)
            thread.start()
            
            # Return immediately - frontend polls for progress
            return {'status': 'started', 'message': 'Analysis started'}
                
        except Exception as e:
            logger.error(f"Error analyzing folder: {e}")
            self._analysis_running = False
            return {'success': False, 'error': str(e)}
    
    def get_analysis_progress(self):
        """Return current analysis progress for frontend polling.
        
        Returns a dict with:
          - running: bool - whether analysis is still in progress
          - percent: int (0-100) - current progress percentage
          - status: str - human-readable status message
          - films_processed: int - number of films processed so far
          - films_total: int - total number of films
          - speed: float - films per second
          - eta_seconds: float - estimated seconds remaining
          - result: dict or None - analysis result when complete
          - error: str or None - error message when complete with error
        """
        response = {
            'running': self._analysis_running,
            'percent': self._analysis_progress,
            'status': self._analysis_status,
            'films_processed': self._films_processed,
            'films_total': self._films_total,
            'speed': self._films_speed,
            'eta_seconds': self._eta_seconds
        }
        # Include result/error when analysis is complete
        if not self._analysis_running:
            if self._analysis_result:
                response['result'] = self._analysis_result
            if self._analysis_error:
                response['error'] = self._analysis_error
        return response
    
    def _count_only_result(self, folder_path):
        """Return result with film count only (no TMDB analysis)."""
        try:
            from src.folder_import import FolderDataLoader
            loader = FolderDataLoader()
            folder_data = loader.load_folder(folder_path)
            parser = loader.parser
            watched_films = parser.extract_unique_films(folder_data['watched'])
            diary_films = parser.extract_unique_films(folder_data['diary'])
            unique_watched = set(name.lower().strip() for name, year in watched_films)
            unique_diary = set(name.lower().strip() for name, year in diary_films)
            films_count = len(unique_watched) + len(unique_diary)
        except Exception:
            films_count = 0
        
        scraped_when = __import__('time').strftime("%d/%m/%Y", __import__('time').localtime())
        self.app_context.stats_data.set_meta_data(films_count, 0.0, 0.0, scraped_when)
        
        return self._build_result()
    
    def _build_result(self):
        """Build result dictionary from current stats data."""
        stats = self.app_context.stats_data
        
        return {
            'success': True,
            'username': self.login_input,
            'films_count': stats.films_count,
            'total_hours': stats.total_hours,
            'total_days': stats.total_days,
            'scraped_at': stats.gui_scraped_at or __import__('time').strftime("%d/%m/%Y", __import__('time').localtime()),
            'countries': json.dumps(stats.country_dict),
            'languages': json.dumps(stats.lang_dict),
            'genres': json.dumps(stats.genre_dict),
            'directors': json.dumps(stats.director_dict),
            'actors': json.dumps(stats.actor_dict),
            'decades': json.dumps(dict(stats.decade_dict)),
            # Diary analytics (nested object format for frontend)
            'diary_data': {
                'weekday': dict(stats.diary_weekday_counts) if hasattr(stats, 'diary_weekday_counts') else {},
                'month': dict(stats.diary_month_counts) if hasattr(stats, 'diary_month_counts') else {},
                'year': dict(stats.diary_year_counts) if hasattr(stats, 'diary_year_counts') else {}
            },
            # Financial analytics (nested object format for frontend)
            'financial_data': {
                'budget': dict(stats.film_budget_data) if hasattr(stats, 'film_budget_data') else {},
                'boxoffice': dict(stats.film_boxoffice_data) if hasattr(stats, 'film_boxoffice_data') else {}
            }
        }
    
    def load_snapshot(self):
        """Open file dialog and load a saved LePrAn JSON snapshot.
        
        Returns:
            dict with 'success', 'result' on success, or 'error' on failure.
        """
        try:
            import tkinter as tk
            from tkinter import filedialog
            
            root = tk.Tk()
            root.withdraw()
            file_path = filedialog.askopenfilename(
                title="Open LePrAn Application Snapshot",
                filetypes=[("JSON Files", "*.json")],
                initialdir=os.path.abspath('.')
            )
            root.destroy()
            
            logger.info(f"load_snapshot: selected file_path={file_path}")
            
            if not file_path:
                return {'success': False, 'error': 'No file selected'}
            
            # Reset data
            self.app_context.stats_data.reset()
            self.app_context.gui_models.clear_all()
            
            # Create importer and import
            from src.snapshot_import import SnapshotImporter, SnapshotImportError
            importer = SnapshotImporter(
                self.app_context.stats_data,
                self.app_context.gui_models
            )
            
            result = importer.import_from_file(file_path, rebuild_from_films=True)
            
            # Extract username from result or filename
            self.login_input = result.get('username', os.path.splitext(os.path.basename(file_path))[0])
            
            # Generate GUI strings
            self.data_manager.generate_gui_strings(self.app_context.stats_data.films_count)
            
            # Build analytics dict for frontend consumption
            stats = self.app_context.stats_data
            analytics_result = {
                'username': self.login_input,
                'films_count': stats.films_count,
                'total_hours': stats.total_hours,
                'total_days': stats.total_days,
                'scraped_at': stats.gui_scraped_at or '',
                'country_stats': dict(stats.country_dict),
                'language_stats': dict(stats.lang_dict),
                'genre_stats': dict(stats.genre_dict),
                'director_stats': dict(stats.director_dict),
                'actor_stats': dict(stats.actor_dict),
                'decade_stats': dict(stats.decade_dict),
                'weekday_stats': {},
                'rating_stats': {},
                'tag_stats': {},
                # Diary analytics
                'diary_data': {
                    'weekday': dict(stats.diary_weekday_counts) if hasattr(stats, 'diary_weekday_counts') else {},
                    'month': dict(stats.diary_month_counts) if hasattr(stats, 'diary_month_counts') else {},
                    'year': dict(stats.diary_year_counts) if hasattr(stats, 'diary_year_counts') else {}
                },
                # Financial analytics
                'financial_data': {
                    'budget': dict(stats.film_budget_data) if hasattr(stats, 'film_budget_data') else {},
                    'boxoffice': dict(stats.film_boxoffice_data) if hasattr(stats, 'film_boxoffice_data') else {}
                }
            }
            
            # Merge restoration metadata into analytics result
            for k, v in result.items():
                if k not in analytics_result:
                    analytics_result[k] = v
            
            logger.info(f"Snapshot loaded from {file_path}: {analytics_result}")
            return {'success': True, 'result': analytics_result}
                
        except Exception as e:
            logger.error(f"Error loading snapshot: {e}")
            return {'success': False, 'error': str(e)}
    
    def save_snapshot(self, data):
        """Save complete application state as a JSON snapshot.
        
        Args:
            data: dict with keys: username, films_count, total_hours, scraped_at,
                  countries, languages, genres, directors, actors, decades
        """
        try:
            import tkinter as tk
            from tkinter import filedialog
            import time
            
            root = tk.Tk()
            root.withdraw()
            default_name = f"{data.get('username', 'results')}_snapshot.json"
            file_path = filedialog.asksaveasfilename(
                title="Save Application Snapshot",
                defaultextension=".json",
                filetypes=[("JSON Files", "*.json")],
                initialfile=default_name,
                initialdir=os.path.abspath('.')
            )
            root.destroy()
            
            if not file_path:
                return {'success': False, 'error': 'No file selected'}
            
            # Convert array data back to dict format
            countries = {item['name']: item['count'] for item in data.get('countries', [])}
            languages = {item['name']: item['count'] for item in data.get('languages', [])}
            genres = {item['name']: item['count'] for item in data.get('genres', [])}
            directors = {item['name']: item['count'] for item in data.get('directors', [])}
            actors = {item['name']: item['count'] for item in data.get('actors', [])}
            decades = {item['name']: item['count'] for item in data.get('decades', [])}
            
            # Update stats data with saved values
            stats = self.app_context.stats_data
            stats.films_count = data.get('films_count', 0)
            stats.total_hours = data.get('total_hours', 0.0)
            stats.total_days = data.get('total_days', 0.0)
            stats.gui_scraped_at = data.get('scraped_at', '')
            
            # Update dictionaries
            stats.country_dict.clear()
            stats.country_dict.update(countries)
            stats.lang_dict.clear()
            stats.lang_dict.update(languages)
            stats.genre_dict.clear()
            stats.genre_dict.update(genres)
            stats.director_dict.clear()
            stats.director_dict.update(directors)
            stats.actor_dict.clear()
            stats.actor_dict.update(actors)
            stats.decade_dict.clear()
            stats.decade_dict.update(decades)
            
            # Build analytics dict
            analytics = {
                'total_films': data.get('films_count', 0),
                'total_hours': data.get('total_hours', 0.0),
                'total_days': data.get('total_days', 0.0),
                'country_stats': countries,
                'language_stats': languages,
                'genre_stats': genres,
                'director_stats': directors,
                'actor_stats': actors,
                'decade_stats': decades,
                'weekday_stats': {},
                'rating_stats': {},
                'tag_stats': {},
                # Diary analytics (preserve from coordinator if available)
                'diary_data': data.get('diary_data', {
                    'weekday': {},
                    'month': {},
                    'year': {}
                }),
                # Financial analytics (preserve from coordinator if available)
                'financial_data': data.get('financial_data', {
                    'budget': {},
                    'boxoffice': {}
                }),
                'username': data.get('username', ''),
                'scraped_at': data.get('scraped_at', time.strftime("%d/%m/%Y", time.localtime()))
            }
            
            # Get film records, diary entries, and raw CSV content from coordinator
            if hasattr(self, '_coordinator') and self._coordinator:
                films = self._coordinator.get_film_records()
                diary_entries = self._coordinator.get_diary_entries()
                watched_entries = self._coordinator.get_watched_entries()
                raw_watched, raw_diary, raw_watchlist = self._coordinator.get_raw_csv_content()
                watchlist_data = self._coordinator._watchlist_data  # Get watchlist data if available
            else:
                films = {}
                diary_entries = []
                watched_entries = []
                raw_watched = raw_diary = raw_watchlist = ""
                watchlist_data = None
            
            # Create exporter and export
            exporter = SnapshotExporter(self.app_context.stats_data, data.get('username', ''))
            success = exporter.export_stats_to_file(
                output_path=file_path,
                films=films,
                diary_entries=diary_entries,
                watched_entries=watched_entries,
                watchlist_data=watchlist_data,
                raw_watched_csv=raw_watched,
                raw_diary_csv=raw_diary,
                raw_watchlist_csv=raw_watchlist,
                analytics=analytics
            )
            
            if success:
                logger.info(f"Snapshot saved to {file_path}")
                return {'success': True, 'file_path': file_path}
            else:
                return {'success': False, 'error': 'Failed to save snapshot'}
                
        except Exception as e:
            logger.error(f"Error saving snapshot: {e}")
            return {'success': False, 'error': str(e)}


def create_app():
    """Create and run the LePrAn application."""
    
    # Create application context
    app_context = AppContext()
    
    # Get resource path for web UI
    if getattr(sys, 'frozen', False):
        # Running as compiled executable
        app_path = sys._MEIPASS
    else:
        app_path = os.path.dirname(os.path.abspath(__file__))
    
    webui_path = os.path.join(app_path, 'webui')
    html_path = os.path.join(webui_path, 'index.html')
    
    if not os.path.exists(html_path):
        logger.error(f"Web UI files not found at: {html_path}")
        sys.exit(1)
    
    # Create API instance (window will be set after window creation)
    api = WebAPI(app_context)
    
    # Create pywebview window
    logger.info("Starting LePrAn with web-based GUI...")
    
    window = webview.create_window(
        title='LePrAn - Letterboxd Profile Analyzer',
        url=f'file://{html_path}',
        js_api=api,
        width=1200,
        height=850,
        resizable=True,
        background_color='#0d1117'
    )
    
    # Set window reference on API for evaluate_js progress callbacks
    api._window = window
    
    # Start the webview
    webview.start()


if __name__ == "__main__":
    colorama.init()
    create_app()
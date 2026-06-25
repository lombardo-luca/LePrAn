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
from gui.gui_settings import Ui_Dialog as Ui_Dialog_Settings

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
        self._analysis_complete = threading.Event()
        
        # Detailed progress stats
        self._films_processed = 0
        self._films_total = 0
        self._films_speed = 0.0
        self._eta_seconds = 0.0
    
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
    
    def analyze_csv_content(self, csv_content):
        """Analyze CSV content string and return results as dict.
        
        Starts analysis in a background thread and returns immediately.
        Progress can be polled via get_analysis_progress().
        When complete, the result is available via get_analysis_progress()['result'].
        """
        try:
            # Reset data
            self.app_context.stats_data.reset()
            self.app_context.gui_models.clear_all()
            
            # Write CSV to temp file for processing
            import tempfile
            with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8-sig') as f:
                f.write(csv_content)
                temp_path = f.name
            
            # Parse username from content (first line or filename)
            first_line = csv_content.strip().split('\n')[0].lower()
            if 'date' in first_line or 'name' in first_line:
                self.login_input = "letterboxd_export"
            else:
                self.login_input = "csv_analysis"
            
            # Check for TMDB API key
            tmdb_token = self.app_context.config.tmdb_access_token
            if not tmdb_token:
                logger.warning("TMDB access token not configured, loading film count only")
                # Count-only is fast enough to run synchronously
                result = self._count_only_result(temp_path)
                try:
                    os.unlink(temp_path)
                except OSError:
                    pass
                return result
            
            # Reset progress state
            self._analysis_progress = 0
            self._analysis_status = 'Starting analysis...'
            self._analysis_running = True
            self._analysis_result = None
            self._analysis_error = None
            self._analysis_complete.clear()
            
            # Create scraper with progress callback
            scraper_with_callback = TMDbScraper(self.app_context, progress_callback=self._update_progress)
            
            def run_analysis():
                try:
                    scraper_with_callback.scrape_csv_file(temp_path)
                    self.data_manager.generate_gui_strings(self.app_context.stats_data.films_count)
                    self._analysis_result = self._build_result()
                except Exception as e:
                    logger.error(f"Error analyzing CSV: {e}")
                    self._analysis_error = str(e)
                finally:
                    # Clean up temp file
                    try:
                        os.unlink(temp_path)
                    except OSError:
                        pass
                    self._analysis_running = False
                    self._analysis_complete.set()
            
            # Start analysis in background thread - return immediately
            thread = threading.Thread(target=run_analysis, daemon=True)
            thread.start()
            
            # Return immediately - frontend polls for progress
            return {'status': 'started', 'message': 'Analysis started'}
                
        except Exception as e:
            logger.error(f"Error analyzing CSV: {e}")
            self._analysis_running = False
            self._analysis_complete.set()
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
    
    def _count_only_result(self, csv_path):
        """Return result with film count only (no TMDB analysis)."""
        films = self.scraper.parse_csv_file(csv_path)
        unique_films = set(name.lower().strip() for name, year in films)
        films_count = len(unique_films)
        
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
            'decades': json.dumps(dict(stats.decade_dict))
        }
    
    def load_saved_csv(self):
        """Open file dialog and load a saved LePrAn CSV file."""
        try:
            import tkinter as tk
            from tkinter import filedialog
            
            root = tk.Tk()
            root.withdraw()
            file_path = filedialog.askopenfilename(
                title="Open LePrAn Saved CSV",
                filetypes=[("CSV Files", "*.csv")],
                initialdir=os.path.abspath('.')
            )
            root.destroy()
            
            logger.info(f"load_saved_csv: selected file_path={file_path}")
            
            if not file_path:
                return {'success': False, 'error': 'No file selected'}
            
            # Reset data
            self.app_context.stats_data.reset()
            self.app_context.gui_models.clear_all()
            
            # Load stats from CSV
            meta = self.data_manager.load_stats_from_csv(file_path)
            
            logger.info(f"load_saved_csv: meta loaded - films_num={getattr(meta, 'films_num', 'N/A')}, total_hours={getattr(meta, 'total_hours', 'N/A')}, total_days={getattr(meta, 'total_days', 'N/A')}")
            
            if not meta:
                return {'success': False, 'error': 'Failed to load CSV data'}
            
            # Extract username
            self.login_input = meta.username if meta.username else os.path.splitext(os.path.basename(file_path))[0]
            
            # Generate GUI strings
            self.data_manager.generate_gui_strings(meta.films_num)
            
            # Build result with loaded data
            stats = self.app_context.stats_data
            logger.info(f"load_saved_csv: stats.total_hours={stats.total_hours}, stats.films_count={stats.films_count}")
            result = {
                'success': True,
                'username': self.login_input,
                'films_count': meta.films_num,
                'total_hours': meta.total_hours,
                'total_days': meta.total_days,
                'scraped_at': meta.scraped_at or __import__('time').strftime("%d/%m/%Y", __import__('time').localtime()),
                'countries': json.dumps(stats.country_dict),
                'languages': json.dumps(stats.lang_dict),
                'genres': json.dumps(stats.genre_dict),
                'directors': json.dumps(stats.director_dict),
                'actors': json.dumps(stats.actor_dict),
                'decades': json.dumps(dict(stats.decade_dict))
            }
            
            logger.info(f"load_saved_csv: returning result with total_hours={result['total_hours']}")
            return result
            
        except Exception as e:
            logger.error(f"Error loading CSV: {e}")
            return {'success': False, 'error': str(e)}
    
    def save_results(self, data):
        """Save results to a CSV file."""
        try:
            import tkinter as tk
            from tkinter import filedialog
            
            root = tk.Tk()
            root.withdraw()
            default_name = f"{data.get('username', 'results')}.csv"
            file_path = filedialog.asksaveasfilename(
                title="Save Statistics CSV",
                defaultextension=".csv",
                filetypes=[("CSV Files", "*.csv")],
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
            
            # Save to CSV using data manager
            success = self.data_manager.save_stats_to_csv(
                username=data.get('username', 'user'),
                scraped_at=data.get('scraped_at', __import__('time').strftime("%d/%m/%Y", __import__('time').localtime())),
                films_num=data.get('films_count', 0),
                total_hours=data.get('total_hours', 0.0),
                total_days=data.get('total_days', 0.0),
                csv_path=file_path
            )
            
            if success:
                return {'success': True}
            else:
                return {'success': False, 'error': 'Failed to save CSV'}
                
        except Exception as e:
            logger.error(f"Error saving results: {e}")
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
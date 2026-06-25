"""
Main window GUI logic.
Handles the main application window and user interactions.
"""
import csv
import os
import time
import logging
from PyQt6 import QtWidgets
from PyQt6.QtWidgets import QHeaderView, QFileDialog
from PyQt6.QtGui import QPixmap, QAction
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from gui.gui_main import Ui_MainWindow
from gui.gui_results import Ui_Dialog
from gui.gui_settings import Ui_Dialog as Ui_Dialog_Settings
from .scraper_tmdb import TMDbScraper
from .data_manager import DataManager


# Configure logging
logger = logging.getLogger(__name__)


class LoginThread(QThread):
    """Thread for running the login/scraping process."""
    doneSignal = pyqtSignal()

    def __init__(self, login: str, app_context):
        super().__init__()
        self.login = login
        self.app_context = app_context
        self.scraper = TMDbScraper(app_context)

    def run(self):
        self.scraper.scrape_user_profile(self.login)
        self.doneSignal.emit()


class TMDBAnalysisThread(QThread):
    """Thread for running the TMDB API analysis process."""
    doneSignal = pyqtSignal()

    def __init__(self, csv_path: str, app_context):
        super().__init__()
        self.csv_path = csv_path
        self.app_context = app_context
        self.scraper = TMDbScraper(app_context)

    def run(self):
        self.scraper.scrape_csv_file(self.csv_path)
        self.doneSignal.emit()


class MainWindow(QtWidgets.QMainWindow, Ui_MainWindow):
    """Main application window."""
    
    def __init__(self, app_context, *args, obj=None, **kwargs):
        super(MainWindow, self).__init__(*args, **kwargs)
        self.app_context = app_context
        self.data_manager = DataManager(app_context)
        self.setupUi(self)

        # Create results window (dialog)
        self.dialog = QtWidgets.QDialog(self)
        self.ui = Ui_Dialog()
        self.ui.setupUi(self.dialog)

        self.change_settings_action = QAction("Change settings", self)
        self.change_settings_action.triggered.connect(self.open_settings_dialog)
        self.menuOptions.addAction(self.change_settings_action)

        # Set pictures (logos)
        self.logo = QPixmap(self.app_context.config.get_resource_path('gfx/logo.png'))
        self.logoSmaller = QPixmap(self.app_context.config.get_resource_path('gfx/logoSmaller.png'))
        self.label.setPixmap(self.logo)
        self.ui.label_logo.setPixmap(self.logoSmaller)
        
        # Connect dialog buttons
        self.ui.pushButton_save.clicked.connect(self.save_results)
        self.ui.pushButton_close.clicked.connect(self.dialog.accept)
        
        self.loginInput = None
        self.lineEdit.setFocusPolicy(Qt.FocusPolicy.ClickFocus)
        
        # Always use TMDB scraper - show CSV file picker
        self.label_3.setText("Select Letterboxd export CSV:")
        self.lineEdit.setVisible(False)
        self.pushButton.setText("Analyze with TMDB")
        
        self.pushButton.clicked.connect(self.analyze)
        # Wire Load button to open-file CSV loader
        self.pushButton_2.clicked.connect(self.load_from_csv)

    def analyze(self):
        """Start analyzing with TMDB API from CSV file."""
        # Reset data for new search
        self.app_context.stats_data.reset()
        self.app_context.gui_models.clear_all()

        self.pushButton.setEnabled(False)
        self.pushButton.setText("Analyzing...")
        
        # Re-enable save button for new analysis
        if hasattr(self, 'ui') and hasattr(self.ui, 'pushButton_save'):
            self.ui.pushButton_save.setEnabled(True)
            self.ui.pushButton_save.setText("Save results")
        
        # Open file dialog to select CSV
        csv_path, _ = QFileDialog.getOpenFileName(
            self,
            "Select Letterboxd Export CSV",
            os.path.abspath('.'),
            "CSV Files (*.csv)"
        )
        
        if not csv_path:
            self.pushButton.setEnabled(True)
            self.pushButton.setText("Analyze with TMDB")
            return
        
        # Set username from filename
        try:
            self.loginInput = os.path.splitext(os.path.basename(csv_path))[0]
        except (OSError, ValueError):
            self.loginInput = "tmdb_analysis"
        
        logger.info(f"Starting TMDB analysis for CSV: {csv_path}")

        # Run TMDB analysis in a thread
        self.thread = TMDBAnalysisThread(csv_path, self.app_context)
        self.thread.doneSignal.connect(self.loginComplete)
        self.thread.start()

    def open_settings_dialog(self):
        """Open the settings dialog."""
        self.dialogSettings = QtWidgets.QDialog(self)
        self.settings = Ui_Dialog_Settings()
        self.settings.setupUi(self.dialogSettings)
        self.settings.spinBox.setValue(int(self.app_context.config.max_threads))

        # Hide scraper profile label since only TMDB is available
        self.settings.label_3.setVisible(False)

        def save():
            self.app_context.config.max_threads = self.settings.spinBox.value()
            self.app_context.config.save_config()
            logger.info(f"Settings saved - max_threads: {self.app_context.config.max_threads}")

        self.settings.save_button = QtWidgets.QDialogButtonBox.StandardButton.Save
        self.dialogSettings.accepted.connect(save)
        self.dialogSettings.show()

    def loginComplete(self):
        """Handle completion of TMDB analysis process."""
        # Re-enable the Analyze button for new searches
        self.pushButton.setText("Analyze with TMDB")
        self.pushButton.setEnabled(True)
        
        # Generate GUI strings
        self.data_manager.generate_gui_strings(self.app_context.stats_data.films_count)
        
        # Update dialog labels
        self.ui.label_username.setText("User: " + self.loginInput)
        self.ui.label_results.setText(self.app_context.stats_data.gui_watched1)
        self.ui.label_results2.setText(self.app_context.stats_data.gui_watched2)
        # Scraped date label
        self.ui.label_5.setText(self.app_context.stats_data.gui_scraped_at or "-")

        # Populate GUI models
        self._populate_gui_models()

        self.dialog.show()

    def _populate_gui_models(self):
        """Populate GUI models with current statistics."""
        # Populate each model
        self.app_context.gui_models.populate_model('countries', self.app_context.stats_data.country_dict, self.app_context.stats_data.films_count, self.app_context.config.list_delim)
        self.app_context.gui_models.populate_model('languages', self.app_context.stats_data.lang_dict, self.app_context.stats_data.films_count, self.app_context.config.list_delim)
        self.app_context.gui_models.populate_model('genres', self.app_context.stats_data.genre_dict, self.app_context.stats_data.films_count, self.app_context.config.list_delim)
        self.app_context.gui_models.populate_model('directors', self.app_context.stats_data.director_dict, self.app_context.stats_data.films_count, self.app_context.config.list_delim)
        self.app_context.gui_models.populate_model('actors', self.app_context.stats_data.actor_dict, self.app_context.stats_data.films_count, self.app_context.config.list_delim)

        # Set models in table views
        self.ui.tableView_1.setModel(self.app_context.gui_models.get_model('countries'))
        self.header1 = self.ui.tableView_1.horizontalHeader()       
        self.header1.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.header1.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        self.header1.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)

        self.ui.tableView_2.setModel(self.app_context.gui_models.get_model('languages'))
        self.header2 = self.ui.tableView_2.horizontalHeader()       
        self.header2.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.header2.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        self.header2.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)

        self.ui.tableView_3.setModel(self.app_context.gui_models.get_model('genres'))
        self.header3 = self.ui.tableView_3.horizontalHeader()       
        self.header3.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.header3.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        self.header3.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)

        self.ui.tableView_botLeft.setModel(self.app_context.gui_models.get_model('directors'))
        self.header4 = self.ui.tableView_botLeft.horizontalHeader()       
        self.header4.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.header4.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        self.header4.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)

        self.ui.tableView_botCenter.setModel(self.app_context.gui_models.get_model('actors'))
        self.header5 = self.ui.tableView_botCenter.horizontalHeader()       
        self.header5.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.header5.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        self.header5.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)

    def _is_lepran_format(self, csv_path: str) -> bool:
        """Check if CSV file is in LePrAn saved format (has META section) vs Letterboxd export.
        
        LePrAn saved CSV format contains sections like:
          META, LANGUAGE, COUNTRY, GENRE, DIRECTOR, ACTOR, DECADE
        
        Letterboxd export CSV has headers like:
          Date, Name, Year, Letterboxd URI (or variants)
        """
        try:
            with open(csv_path, 'r', encoding='utf-8-sig') as f:
                content = f.read()
            lines = [l.strip().upper() for l in content.strip().split('\n') if l.strip()]
            
            lepran_indicators = {'META', 'HOURS', 'DAYS', 'LANGUAGE', 'COUNTRY', 'GENRE', 'DIRECTOR', 'ACTOR', 'DECADE'}
            letterboxd_indicators = {'DATE', 'NAME', 'YEAR', 'LETTERBOXD URI'}
            
            lepran_count = sum(1 for l in lines if l in lepran_indicators)
            letterboxd_count = sum(1 for l in lines if l in letterboxd_indicators)
            
            # If we find META or HOURS, it's definitely LePrAn format
            if lepran_count >= 2:
                logger.debug(f"Detected LePrAn format (lepran_indicators={lepran_count})")
                return True
            # If we find Letterboxd header columns and no LePrAn indicators
            if letterboxd_count >= 2 and lepran_count == 0:
                logger.debug(f"Detected Letterboxd format (letterboxd_indicators={letterboxd_count})")
                return False
            # Default: assume LePrAn (safer, preserves runtime data)
            logger.debug(f"Could not determine CSV format definitively (lepran={lepran_count}, letterboxd={letterboxd_count}), assuming LePrAn format")
            return True
        except Exception as e:
            logger.warning(f"Could not detect CSV format, assuming LePrAn format: {e}")
            return True  # Changed: default to LePrAn (safer, preserves runtime data)

    def load_from_csv(self):
        """Load statistics from a CSV file.
        
        Supports two formats:
        1. LePrAn saved CSV (with META/LANGUAGE/COUNTRY sections) - loaded directly into GUI
        2. Letterboxd export CSV (Date,Name,Year,URI) - processed through TMDB scraper for full analysis
        """
        # Open file dialog restricted to CSV files
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Open Letterboxd/LePrAn CSV",
            os.path.abspath('.'),
            "CSV Files (*.csv)"
        )
        if not file_path:
            return
        
        logger.info(f"load_from_csv: selected file_path={file_path}")
        
        # Re-enable save button for new file load
        if hasattr(self, 'ui') and hasattr(self.ui, 'pushButton_save'):
            self.ui.pushButton_save.setEnabled(True)
            self.ui.pushButton_save.setText("Save results")
        
        # Detect CSV format and route accordingly
        is_lepran_format = self._is_lepran_format(file_path)
        logger.info(f"load_from_csv: is_lepran_format={is_lepran_format}")
        
        if is_lepran_format:
            # LePrAn saved format - load directly without TMDB API
            logger.info(f"Detected LePrAn saved CSV format: {file_path}")
            meta = self.data_manager.load_stats_from_csv(file_path)
            logger.info(f"load_from_csv: meta loaded - films_num={getattr(meta, 'films_num', 'N/A')}, total_hours={getattr(meta, 'total_hours', 'N/A')}, total_days={getattr(meta, 'total_days', 'N/A')}")
            
            # Extract username from LoadedStats object
            if hasattr(meta, 'username') and meta.username:
                self.loginInput = meta.username
            else:
                try:
                    self.loginInput = os.path.splitext(os.path.basename(file_path))[0]
                except (OSError, ValueError):
                    self.loginInput = "(loaded)"
        else:
            # Letterboxd export format - process through TMDB scraper
            logger.info(f"Detected Letterboxd export CSV format: {file_path}")
            
            # Check for TMDB API key
            tmdb_token = self.app_context.config.tmdb_access_token
            if not tmdb_token:
                logger.warning("TMDB access token not configured, loading film count only")
                # Fallback: just count films without TMDB analysis
                self._load_letterboxd_csv_count_only(file_path)
                return
            
            # Run TMDB analysis in a thread
            self.pushButton.setEnabled(False)
            self.pushButton.setText("Analyzing with TMDB...")
            
            self.thread = TMDBAnalysisThread(file_path, self.app_context)
            self.thread.doneSignal.connect(self._tmdbLoadComplete)
            self.thread.start()

    def _load_letterboxd_csv_count_only(self, csv_path: str):
        """Load Letterboxd CSV and count films without TMDB API (fallback mode).
        
        Also attempts to extract runtime from META section if present (LePrAn format fallback).
        """
        scraper = TMDbScraper(self.app_context)
        films = scraper.parse_csv_file(csv_path)
        
        films_count = 0
        total_hours = 0.0
        
        if films:
            # Count unique films (some exports may have duplicates)
            unique_films = set(name.lower().strip() for name, year in films)
            films_count = len(unique_films)
            logger.info(f"Loaded {films_count} unique films from Letterboxd CSV (count-only mode)")
        else:
            # No films parsed - check if this might actually be a LePrAn format CSV
            # that was misidentified. Try to extract META values directly.
            logger.warning(f"No films found in Letterboxd CSV: {csv_path}. "
                          f"Attempting to parse as LePrAn format...")
            films_count, total_hours = self._extract_lepran_meta_from_csv(csv_path)
            if films_count > 0 or total_hours > 0:
                logger.info(f"Successfully extracted LePrAn META: {films_count} films, {total_hours:.6f} hours")
            else:
                logger.warning("No valid data found in CSV file")
        
        # Set basic stats
        scraped_when = time.strftime("%d/%m/%Y", time.localtime())
        total_days = total_hours / 24.0
        self.app_context.stats_data.set_meta_data(films_count, total_hours, total_days, scraped_when)
        
        try:
            self.loginInput = os.path.splitext(os.path.basename(csv_path))[0]
        except (OSError, ValueError):
            self.loginInput = "letterboxd_export"
        
        # Show results dialog
        self.loginComplete()

    def _extract_lepran_meta_from_csv(self, csv_path: str):
        """Extract META values (films_count, total_hours) from a LePrAn-format CSV.
        
        This is a fallback parser for when the format detection fails and the CSV
        is actually in LePrAn saved format (with META/LANGUAGE/COUNTRY sections).
        
        Returns:
            tuple: (films_count, total_hours)
        """
        films_count = 0
        total_hours = 0.0
        
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                for row in reader:
                    if len(row) < 3:
                        continue
                    section, name, count = row[0].strip().upper(), row[1].strip().upper(), row[2].strip()
                    
                    if section == 'META':
                        if name == 'FILMS':
                            try:
                                films_count = int(count)
                            except ValueError:
                                films_count = 0
                        elif name == 'HOURS':
                            try:
                                total_hours = float(count)
                            except ValueError:
                                total_hours = 0.0
        except Exception as e:
            logger.warning(f"Failed to extract LePrAn META from CSV: {e}")
        
        return films_count, total_hours

    def _tmdbLoadComplete(self):
        """Handle completion of TMDB analysis from CSV load."""
        self.pushButton.setEnabled(True)
        self.pushButton.setText("Analyze")
        
        # Extract username from filename
        try:
            self.loginInput = os.path.splitext(os.path.basename(
                getattr(self.thread, 'csv_path', '')
            ))[0]
        except (OSError, ValueError):
            self.loginInput = "tmdb_analysis"
        
        # Show results dialog
        self.loginComplete()

    def save_results(self):
        """Save current statistics to CSV file."""
        # Open file dialog to choose save location and filename
        username = self.loginInput or "user"
        default_filename = f"{username}.csv"
        
        file_path, _ = QFileDialog.getSaveFileName(
            self.dialog,
            "Save statistics CSV",
            default_filename,
            "CSV Files (*.csv);;All Files (*)"
        )
        
        if not file_path:
            return  # User cancelled the dialog
        
        # Save the current statistics to CSV
        scraped_at = self.app_context.stats_data.gui_scraped_at or time.strftime("%d/%m/%Y", time.localtime())
        success = self.data_manager.save_stats_to_csv(
            username,
            scraped_at,
            self.app_context.stats_data.films_count,
            self.app_context.stats_data.total_hours,
            self.app_context.stats_data.total_days,
            csv_path=file_path,
        )
        
        if success:
            # Update button text to show success
            self.ui.pushButton_save.setText("Results saved")
            self.ui.pushButton_save.setEnabled(False)  # Disable button after saving

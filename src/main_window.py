"""
Main window GUI logic.
Handles the main application window and user interactions.
"""
import os
import time
from PyQt6 import QtWidgets
from PyQt6.QtWidgets import QHeaderView, QFileDialog
from PyQt6.QtGui import QPixmap, QAction
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from gui.gui_main import Ui_MainWindow
from gui.gui_results import Ui_Dialog
from gui.gui_settings import Ui_Dialog as Ui_Dialog_Settings
from .scraper import LetterboxdScraper
from .scraper_legacy import LegacyLetterboxdScraper
from .data_manager import DataManager


class LoginThread(QThread):
    """Thread for running the login/scraping process."""
    doneSignal = pyqtSignal()

    def __init__(self, login: str, app_context):
        super().__init__()
        self.login = login
        self.app_context = app_context
        
        # Select scraper based on configuration
        if app_context.config.scraper == "legacy":
            self.scraper = LegacyLetterboxdScraper(app_context)
        else:  # Default to optimized scraper
            self.scraper = LetterboxdScraper(app_context)

    def run(self):
        result = self.scraper.scrape_user_profile(self.login)
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
        self.pushButton.clicked.connect(self.analyze)
        # Wire Load button to open-file CSV loader
        self.pushButton_2.clicked.connect(self.load_from_csv)

    def analyze(self):
        """Start analyzing a user's Letterboxd profile."""
        # Reset data for new search
        self.app_context.stats_data.reset()
        self.app_context.gui_models.clear_all()

        self.pushButton.setEnabled(False)
        self.pushButton.setText("Analyzing...")
        
        # Re-enable save button for new analysis
        if hasattr(self, 'ui') and hasattr(self.ui, 'pushButton_save'):
            self.ui.pushButton_save.setEnabled(True)
            self.ui.pushButton_save.setText("Save results")
        
        self.loginInput = self.lineEdit.text()
        print("Name: " + self.loginInput)

        # Run login function inside of a thread
        self.thread = LoginThread(self.loginInput, self.app_context)
        self.thread.doneSignal.connect(self.loginComplete)
        self.thread.start()

    def open_settings_dialog(self):
        """Open the settings dialog."""
        self.dialogSettings = QtWidgets.QDialog(self)
        self.settings = Ui_Dialog_Settings()
        self.settings.setupUi(self.dialogSettings)
        self.settings.spinBox.setValue(int(self.app_context.config.max_threads))
        print("Max threads1: " + str(self.app_context.config.max_threads))
        
        def save():
            self.app_context.config.max_threads = self.settings.spinBox.value()
            self.app_context.config.save_config()
            print("Max threads2: " + str(self.app_context.config.max_threads))
        
        self.settings.save_button = QtWidgets.QDialogButtonBox.StandardButton.Save
        self.dialogSettings.accepted.connect(save)
        self.dialogSettings.show()

    def loginComplete(self):
        """Handle completion of login/scraping process."""
        # Re-enable the Analyze button for new searches
        self.pushButton.setText("Analyze")
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

    def load_from_csv(self):
        """Load statistics from a CSV file."""
        # Open file dialog restricted to CSV files
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Open statistics CSV",
            os.path.abspath('.'),
            "CSV Files (*.csv)"
        )
        if not file_path:
            return
        
        # Re-enable save button for new file load
        if hasattr(self, 'ui') and hasattr(self.ui, 'pushButton_save'):
            self.ui.pushButton_save.setEnabled(True)
            self.ui.pushButton_save.setText("Save results")
        
        meta = self.data_manager.load_stats_from_csv(file_path)
        # Set username label from CSV contents if present; fallback to filename
        loaded_user = (meta or {}).get('username') if isinstance(meta, dict) else ''
        if loaded_user:
            self.loginInput = loaded_user
        else:
            try:
                self.loginInput = os.path.splitext(os.path.basename(file_path))[0]
            except Exception:
                self.loginInput = "(loaded)"
        
        # Show results dialog using existing setup
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

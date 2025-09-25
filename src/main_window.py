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
from .config import config
from .data_models import stats_data, gui_models
from .scraper import LetterboxdScraper
from .file_manager import FileManager


class LoginThread(QThread):
    """Thread for running the login/scraping process."""
    doneSignal = pyqtSignal()

    def __init__(self, login):
        super().__init__()
        self.login = login
        self.scraper = LetterboxdScraper()

    def run(self):
        result = self.scraper.scrape_user_profile(self.login)
        self.doneSignal.emit()


class MainWindow(QtWidgets.QMainWindow, Ui_MainWindow):
    """Main application window."""
    
    def __init__(self, *args, obj=None, **kwargs):
        super(MainWindow, self).__init__(*args, **kwargs)
        self.setupUi(self)

        # Create results window (dialog)
        self.dialog = QtWidgets.QDialog(self)
        self.ui = Ui_Dialog()
        self.ui.setupUi(self.dialog)

        self.change_settings_action = QAction("Change settings", self)
        self.change_settings_action.triggered.connect(self.open_settings_dialog)
        self.menuOptions.addAction(self.change_settings_action)

        # Set pictures (logos)
        self.logo = QPixmap(config.get_resource_path('gfx/logo.png'))
        self.logoSmaller = QPixmap(config.get_resource_path('gfx/logoSmaller.png'))
        self.label.setPixmap(self.logo)
        self.ui.label_logo.setPixmap(self.logoSmaller)
        
        # Connect dialog save button
        self.ui.pushButton.clicked.connect(self.save_results)
        
        self.loginInput = None
        self.lineEdit.setFocusPolicy(Qt.FocusPolicy.ClickFocus)
        self.pushButton.clicked.connect(self.analyze)
        # Wire Load button to open-file CSV loader
        self.pushButton_2.clicked.connect(self.load_from_csv)

    def analyze(self):
        """Start analyzing a user's Letterboxd profile."""
        # Reset data for new search
        stats_data.reset()
        gui_models.clear_all()

        self.pushButton.setEnabled(False)
        self.pushButton.setText("Analyzing...")
        
        # Re-enable save button for new analysis
        if hasattr(self, 'ui') and hasattr(self.ui, 'pushButton'):
            self.ui.pushButton.setEnabled(True)
            self.ui.pushButton.setText("Save results")
        
        self.loginInput = self.lineEdit.text()
        print("Name: " + self.loginInput)

        # Run login function inside of a thread
        self.thread = LoginThread(self.loginInput)
        self.thread.doneSignal.connect(self.loginComplete)
        self.thread.start()

    def open_settings_dialog(self):
        """Open the settings dialog."""
        self.dialogSettings = QtWidgets.QDialog(self)
        self.settings = Ui_Dialog_Settings()
        self.settings.setupUi(self.dialogSettings)
        self.settings.spinBox.setValue(int(config.max_threads))
        print("Max threads1: " + str(config.max_threads))
        
        def save():
            config.max_threads = self.settings.spinBox.value()
            config.save_config()
            print("Max threads2: " + str(config.max_threads))
        
        self.settings.save_button = QtWidgets.QDialogButtonBox.StandardButton.Save
        self.dialogSettings.accepted.connect(save)
        self.dialogSettings.show()

    def loginComplete(self):
        """Handle completion of login/scraping process."""
        # Re-enable the Analyze button for new searches
        self.pushButton.setText("Analyze")
        self.pushButton.setEnabled(True)
        
        # Generate GUI strings
        FileManager.generate_gui_strings(stats_data.films_count)
        
        # Update dialog labels
        self.ui.label_username.setText("User: " + self.loginInput)
        self.ui.label_results.setText(stats_data.gui_watched1)
        self.ui.label_results2.setText(stats_data.gui_watched2)
        # Scraped date label
        self.ui.label_5.setText(stats_data.gui_scraped_at or "-")

        # Populate GUI models
        self._populate_gui_models()

        self.dialog.show()

    def _populate_gui_models(self):
        """Populate GUI models with current statistics."""
        # Populate each model
        gui_models.populate_model('countries', stats_data.country_dict, stats_data.films_count, config.list_delim)
        gui_models.populate_model('languages', stats_data.lang_dict, stats_data.films_count, config.list_delim)
        gui_models.populate_model('genres', stats_data.genre_dict, stats_data.films_count, config.list_delim)
        gui_models.populate_model('directors', stats_data.director_dict, stats_data.films_count, config.list_delim)
        gui_models.populate_model('actors', stats_data.actor_dict, stats_data.films_count, config.list_delim)

        # Set models in table views
        self.ui.tableView_1.setModel(gui_models.get_model('countries'))
        self.header1 = self.ui.tableView_1.horizontalHeader()       
        self.header1.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.header1.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        self.header1.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)

        self.ui.tableView_2.setModel(gui_models.get_model('languages'))
        self.header2 = self.ui.tableView_2.horizontalHeader()       
        self.header2.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.header2.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        self.header2.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)

        self.ui.tableView_3.setModel(gui_models.get_model('genres'))
        self.header3 = self.ui.tableView_3.horizontalHeader()       
        self.header3.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.header3.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        self.header3.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)

        self.ui.tableView_botLeft.setModel(gui_models.get_model('directors'))
        self.header4 = self.ui.tableView_botLeft.horizontalHeader()       
        self.header4.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.header4.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        self.header4.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)

        self.ui.tableView_botCenter.setModel(gui_models.get_model('actors'))
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
        if hasattr(self, 'ui') and hasattr(self.ui, 'pushButton'):
            self.ui.pushButton.setEnabled(True)
            self.ui.pushButton.setText("Save results")
        
        meta = FileManager.load_stats_from_csv(file_path)
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
        scraped_at = stats_data.gui_scraped_at or time.strftime("%d/%m/%Y", time.localtime())
        success = FileManager.save_stats_to_csv(
            username,
            scraped_at,
            stats_data.films_count,
            stats_data.total_hours,
            stats_data.total_days,
            csv_path=file_path,
        )
        
        if success:
            # Update button text to show success
            self.ui.pushButton.setText("Results saved")
            self.ui.pushButton.setEnabled(False)  # Disable button after saving

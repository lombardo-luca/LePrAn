"""
LePrAn - Letterboxd Profile Analyzer
Main application entry point.
"""
import sys
import logging
from PyQt6 import QtWidgets, QtGui
from src.context import AppContext
from src.main_window import MainWindow


# Configure logging for the application
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Main application entry point."""
    try:
        # Create application context for dependency injection
        app_context = AppContext()
        
        app = QtWidgets.QApplication(sys.argv)
        
        # Set application-wide window icon
        try:
            app_icon = QtGui.QIcon("gfx/icon.png")
            app.setWindowIcon(app_icon)
        except Exception as e:
            logger.warning(f"Could not load application icon: {e}")
        
        # Pass context to main window instead of using global state
        window = MainWindow(app_context)
        window.show()
        
        logger.info("LePrAn application started successfully")
        app.exec()
        
    except Exception as e:
        logger.error(f"Critical error starting application: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
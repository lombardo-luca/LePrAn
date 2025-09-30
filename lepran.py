"""
LePrAn - Letterboxd Profile Analyzer
Main application entry point.
"""
import sys
from PyQt6 import QtWidgets, QtGui
from src.context import AppContext
from src.main_window import MainWindow


def main():
    """Main application entry point."""
    # Create application context for dependency injection
    app_context = AppContext()
    
    app = QtWidgets.QApplication(sys.argv)
    
    # Set application-wide window icon
    app_icon = QtGui.QIcon("gfx/icon.png")
    app.setWindowIcon(app_icon)
    
    # Pass context to main window instead of using global state
    window = MainWindow(app_context)
    window.show()
    app.exec()


if __name__ == "__main__":
    main()
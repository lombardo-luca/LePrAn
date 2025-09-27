"""
LePrAn - Letterboxd Profile Analyzer
Main application entry point.
"""
import sys
from PyQt6 import QtWidgets, QtGui
from src.main_window import MainWindow


def main():
    """Main application entry point."""
    app = QtWidgets.QApplication(sys.argv)
    
    # Set application-wide window icon
    app_icon = QtGui.QIcon("gfx/icon.png")
    app.setWindowIcon(app_icon)
    
    window = MainWindow()
    window.show()
    app.exec()


if __name__ == "__main__":
    main()
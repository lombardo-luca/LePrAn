"""
LePrAn - Letterboxd Profile Analyzer
Main application entry point.
"""
import sys
from PyQt6 import QtWidgets
from src.main_window import MainWindow


def main():
    """Main application entry point."""
    app = QtWidgets.QApplication(sys.argv)
    window = MainWindow()
    window.show()
    app.exec()


if __name__ == "__main__":
    main()
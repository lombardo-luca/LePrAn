@echo off
echo Building LePrAn single-file executable...
pyinstaller --clean LePrAn.spec
echo Build complete! The executable is in the dist/ folder.
pause

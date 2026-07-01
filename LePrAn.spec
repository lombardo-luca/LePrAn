# -*- mode: python ; coding: utf-8 -*-
"""
PyInstaller spec file for LePrAn - Letterboxd Profile Analyzer.

Produces a single-file Windows executable (LePrAn.exe) that bundles:
- All Python modules and dependencies
- Web UI assets (webui/ directory with HTML, CSS, JS, assets)
- Application icon

When run from the packaged executable:
- Web assets are loaded from the PyInstaller temp extraction directory (sys._MEIPASS)
- Configuration is stored beside the executable in config/
- Auxiliary directories (logs/, exports/, cache/) are auto-created beside the executable
"""

block_cipher = None

# Determine the data files to bundle
# webui/ contains index.html, css/, js/, assets/
import os
from pathlib import Path

# Web UI files: webui/ -> webui/
# In spec files, __file__ is not available; use __spec__.origin
_spec_path = os.path.dirname(os.path.abspath(__spec__.origin))
webui_src = os.path.join(_spec_path, 'webui')
webui_datas = []
if os.path.isdir(webui_src):
    for root, dirs, files in os.walk(webui_src):
        for _f in files:
            rel_dir = os.path.relpath(root, _spec_path)
            webui_datas.append((os.path.join(root, _f), rel_dir))

gfx_src = os.path.join(_spec_path, 'gfx')
if os.path.isdir(gfx_src):
    for root, dirs, files in os.walk(gfx_src):
        for _f in files:
            rel_dir = os.path.relpath(root, _spec_path)
            webui_datas.append((os.path.join(root, _f), rel_dir))

a = Analysis(
    ['lepran.py'],
    pathex=[],
    binaries=[],
    datas=webui_datas,
    hiddenimports=[
        'webview',
        'webview.util',
        'webview.dom',
        'webview.dom.command',
        'webview.eventloop',
        'webview.guilib',
        'webview.js_modules',
        'webview.parsers',
        'webview.ssl_',
        'webview.windows',
        'webview.webkit',
        'webview.qt',
        'colorama',
        'colorama.ansitowin32',
        'colorama.winterm',
        'requests',
        'requests.adapters',
        'requests.models',
        'urllib3',
        'urllib3.util',
        'urllib3.connection',
        'urllib3.connectionpool',
        'urllib3.util',
        'urllib3.util.retry',
        'urllib3.util.ssl_',
        'urllib3.util.timeout',
        'urllib3.util.url',
        'urllib3.util.response',
        'urllib3.util.ssltransport',
        'chardet',
        'chardet.universaldetector',
        'idna',
        'certifi',
        'certifi.core',
        'sqlite3',
        'json',
        'csv',
        'threading',
        'time',
        'pathlib',
        'logging',
        'logging.config',
        'logging.handlers',
        'email',
        'email.mime',
        'html.parser',
        'http',
        'http.client',
        'urllib',
        'xml',
        'xml.etree',
        'xml.etree.ElementTree',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'unittest',
        'unittest',
        'email.mime.multipart',
        'email.mime.text',
        'email.mime.base',
        'setuptools',
        'numpy',
        'pandas',
        'matplotlib',
        'scipy',
    ],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

# Get icon path (optional - icon.png needs to be .ico format for PyInstaller)
icon_path = os.path.join(_spec_path, 'gfx', 'icon.ico')
icon = icon_path if os.path.exists(icon_path) else None

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='LePrAn',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=icon,
)
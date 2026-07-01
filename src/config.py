"""
Configuration management.
Handles reading and writing configuration settings.

Configuration is stored in a portable ``cfg/`` directory located next to the
executable (when frozen) or next to the project root (when running from
source).  The directory and its files are created automatically.
"""
import os
import sys
import logging
from pathlib import Path

import requests

# Configure logging
logger = logging.getLogger(__name__)


def _get_application_root():
    """Return the directory that contains the running application.

    * **Frozen (PyInstaller exe)** → ``os.path.dirname(sys.executable)``
    * **Running from source**      → directory containing the top-level
      ``lepran.py`` script (i.e. the project root).
    """
    if getattr(sys, 'frozen', False):
        # Bundled exe – cfg/ lives next to LePrAn.exe
        return os.path.dirname(sys.executable)

    # Running from source – walk up from *this* file (src/config.py) to the
    # project root which is one level above ``src/``.
    return str(Path(__file__).resolve().parent.parent)


class Config:
    """Configuration management class."""

    def __init__(self):
        self.list_delim = 200
        self.scraper_profile = "tmdb"  # Only TMDB API is now supported
        self.tmdb_access_token = ""
        self.config_dir = self._resolve_config_dir()
        self.config_path = os.path.join(self.config_dir, 'config.txt')
        self.env_path = os.path.join(self.config_dir, '.env')
        # Ensure the cfg directory exists on first run
        os.makedirs(self.config_dir, exist_ok=True)
        self._load_env()
        self.load_config()

    # ------------------------------------------------------------------
    # Config directory resolution
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_config_dir():
        """Return the writable configuration directory (``cfg/``).

        Resolution order:
        1. ``LEPRAN_CONFIG_DIR`` environment variable (if set).
        2. ``<application_root>/cfg/``
        """
        override = os.environ.get('LEPRAN_CONFIG_DIR')
        if override:
            return os.path.abspath(os.path.expanduser(override))

        return os.path.join(_get_application_root(), 'cfg')

    # ------------------------------------------------------------------
    # .env loading
    # ------------------------------------------------------------------

    def _load_env(self):
        """Load TMDB API key / access token from .env file.

        Prefers ``TMDB_API_KEY`` (API key) over ``TMDB_ACCESS_TOKEN``
        (OAuth token), since the TMDB v3 API requires an API key.

        Search order:
        1. ``cfg/.env``  (canonical portable location)
        """
        env_path = os.path.normpath(self.env_path)
        if not os.path.exists(env_path):
            logger.info("No .env file found in cfg/ – TMDB API key not configured yet.")
            return

        try:
            with open(env_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line.startswith('TMDB_API_KEY='):
                        self.tmdb_access_token = self._normalize_env_value(line.split('=', 1)[1])
                        logger.info(f"TMDB API key loaded from {env_path}")
                        return
                    elif line.startswith('TMDB_ACCESS_TOKEN=') and not self.tmdb_access_token:
                        self.tmdb_access_token = self._normalize_env_value(line.split('=', 1)[1])
                        logger.info(f"TMDB access token (fallback) loaded from {env_path}")
        except IOError as e:
            logger.warning(f"Error reading .env file: {e}")

    @staticmethod
    def _normalize_env_value(value):
        """Normalize simple KEY=value entries from .env files."""
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        return value.strip()

    def get_resource_path(self, relative_path):
        """Get the absolute path to a resource file."""
        try:
            base_path = sys._MEIPASS
        except Exception:
            base_path = os.path.abspath(".")
        return os.path.join(base_path, relative_path)

    # ------------------------------------------------------------------
    # config.txt loading / saving
    # ------------------------------------------------------------------

    def load_config(self):
        """Load configuration from file."""
        if os.path.exists(self.config_path):
            try:
                with open(self.config_path, encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if ':' in line:
                            key, value = line.split(':', 1)
                            if key == 'scraperProfile':
                                # scraperProfile key is kept for backward compatibility but only "tmdb" is valid
                                self.scraper_profile = "tmdb"
                logger.info("Config file loaded.")
                logger.debug(f"Config loaded: scraper_profile={self.scraper_profile}")
            except (IOError, ValueError) as e:
                logger.warning(f"Error reading config: {e}")
            except Exception as e:
                logger.error(f"Unexpected error loading config: {e}")
        else:
            self.create_default_config()

    def create_default_config(self):
        """Create default configuration file."""
        try:
            os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
            with open(self.config_path, 'w', encoding='utf-8') as f:
                f.write("scraperProfile:tmdb\n")
            logger.info("Config file created with TMDB API as default.")
        except IOError as e:
            logger.error(f"Error creating config: {e}")
        except Exception as e:
            logger.error(f"Unexpected error creating config: {e}")

    def save_config(self):
        """Save current configuration to file."""
        try:
            os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
            with open(self.config_path, 'w', encoding='utf-8') as f:
                f.write(f"scraperProfile:{self.scraper_profile}\n")
            logger.info("Config saved.")
            logger.debug(f"Config saved: scraper_profile={self.scraper_profile}")
        except IOError as e:
            logger.error(f"Error saving config: {e}")
        except Exception as e:
            logger.error(f"Unexpected error saving config: {e}")

    # ------------------------------------------------------------------
    # TMDB API key management
    # ------------------------------------------------------------------

    def has_tmdb_api_key(self):
        """Return whether a TMDB API key is configured."""
        return bool(self.tmdb_access_token.strip())

    def save_tmdb_api_key(self, api_key):
        """Persist the TMDB API key to ``cfg/.env``."""
        api_key = (api_key or '').strip()
        if not api_key:
            raise ValueError("TMDB API key cannot be empty")

        os.makedirs(self.config_dir, exist_ok=True)
        lines = []
        found_key = False

        if os.path.exists(self.env_path):
            with open(self.env_path, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.startswith('TMDB_API_KEY='):
                        lines.append(f'TMDB_API_KEY={api_key}\n')
                        found_key = True
                    elif line.startswith('TMDB_ACCESS_TOKEN='):
                        continue
                    else:
                        lines.append(line)

        if not found_key:
            if lines and not lines[-1].endswith('\n'):
                lines[-1] = lines[-1] + '\n'
            lines.append(f'TMDB_API_KEY={api_key}\n')

        with open(self.env_path, 'w', encoding='utf-8') as f:
            f.writelines(lines)

        self.tmdb_access_token = api_key
        logger.info(f"TMDB API key saved to {self.env_path}")

    @staticmethod
    def validate_tmdb_api_key(api_key):
        """Validate a TMDB API key by making a lightweight test request.

        Returns ``(True, "")`` on success or ``(False, error_message)``
        on failure.
        """
        api_key = (api_key or '').strip()
        if not api_key:
            return False, "API key cannot be empty."

        try:
            resp = requests.get(
                "https://api.themoviedb.org/3/configuration",
                params={"api_key": api_key},
                timeout=10,
            )
            if resp.status_code == 200:
                return True, ""
            elif resp.status_code == 401:
                return False, "Invalid API key. Please check and try again."
            else:
                return False, f"TMDB returned status {resp.status_code}."
        except requests.exceptions.ConnectionError:
            return False, "Cannot reach TMDB. Check your internet connection."
        except requests.exceptions.Timeout:
            return False, "Request timed out. Please try again."
        except Exception as e:
            return False, f"Unexpected error: {e}"

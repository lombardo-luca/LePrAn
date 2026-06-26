"""
Configuration management.
Handles reading and writing configuration settings.
"""
import os
import sys
import logging


# Configure logging
logger = logging.getLogger(__name__)


class Config:
    """Configuration management class."""
    
    def __init__(self):
        self.list_delim = 200
        self.scraper_profile = "tmdb"  # Only TMDB API is now supported
        self.tmdb_access_token = ""
        self.config_path = self.get_resource_path('cfg/config.txt')
        self._load_env()
        self.load_config()
    
    def _load_env(self):
        """Load TMDB API key / access token from .env file.
        
        Prefers TMDB_API_KEY (API key) over TMDB_ACCESS_TOKEN (OAuth token),
        since the TMDB v3 API requires an API key, not an OAuth access token.
        """
        env_paths = [
            os.path.join(self.get_resource_path('.'), '..', '.env'),
            os.path.join(os.getcwd(), '.env'),
        ]
        for env_path in env_paths:
            env_path = os.path.normpath(env_path)
            if os.path.exists(env_path):
                try:
                    with open(env_path, 'r') as f:
                        for line in f:
                            line = line.strip()
                            # Prefer TMDB_API_KEY (API key) over TMDB_ACCESS_TOKEN (OAuth token)
                            if line.startswith('TMDB_API_KEY='):
                                self.tmdb_access_token = line.split('=', 1)[1].strip()
                                logger.info("TMDB API key loaded from .env")
                                return
                            elif line.startswith('TMDB_ACCESS_TOKEN=') and not self.tmdb_access_token:
                                # Fallback to TMDB_ACCESS_TOKEN if TMDB_API_KEY is not set
                                self.tmdb_access_token = line.split('=', 1)[1].strip()
                                logger.info("TMDB access token (fallback) loaded from .env")
                except IOError as e:
                    logger.warning(f"Error reading .env file: {e}")
                    continue
    
    def get_resource_path(self, relative_path):
        """Get the absolute path to a resource file."""
        try:
            base_path = sys._MEIPASS
        except Exception:
            base_path = os.path.abspath(".")
        return os.path.join(base_path, relative_path)
    
    def load_config(self):
        """Load configuration from file."""
        if os.path.exists(self.config_path):
            try:
                with open(self.config_path) as f:
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
            with open(self.config_path, 'w') as f:
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
            with open(self.config_path, 'w') as f:
                f.write(f"scraperProfile:{self.scraper_profile}\n")
            logger.info("Config saved.")
            logger.debug(f"Config saved: scraper_profile={self.scraper_profile}")
        except IOError as e:
            logger.error(f"Error saving config: {e}")
        except Exception as e:
            logger.error(f"Unexpected error saving config: {e}")
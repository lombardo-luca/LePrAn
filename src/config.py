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
        self.max_threads = 20
        self.list_delim = 200
        self.scraper = "optimized"  # Use "optimized" or "legacy"
        self.config_path = self.get_resource_path('cfg/config.txt')
        self.load_config()
    
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
                            if key == 'workerThreadsNumber':
                                self.max_threads = int(value)
                            elif key == 'scraper':
                                if value.lower() in ['optimized', 'legacy']:
                                    self.scraper = value.lower()
                            elif key == 'useOptimizedScraper':  # Legacy support
                                self.scraper = "optimized" if value.lower() == 'true' else "legacy"
                logger.info("Config file loaded.")
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
                f.write("workerThreadsNumber:20\n")
                f.write("scraper:optimized\n")
            logger.info("Config file created with optimized scraper as default.")
        except IOError as e:
            logger.error(f"Error creating config: {e}")
        except Exception as e:
            logger.error(f"Unexpected error creating config: {e}")
    
    def save_config(self):
        """Save current configuration to file."""
        try:
            os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
            with open(self.config_path, 'w') as f:
                f.write(f"workerThreadsNumber:{self.max_threads}\n")
                f.write(f"scraper:{self.scraper}\n")
            logger.info("Config saved.")
        except IOError as e:
            logger.error(f"Error saving config: {e}")
        except Exception as e:
            logger.error(f"Unexpected error saving config: {e}")


# Note: Global config instance removed in favor of dependency injection.
# Config is now instantiated in AppContext. See src/context.py

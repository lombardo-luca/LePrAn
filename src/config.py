"""
Configuration management.
Handles reading and writing configuration settings.
"""
import os
import sys


class Config:
    """Configuration management class."""
    
    def __init__(self):
        self.max_threads = 20
        self.list_delim = 200
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
                    first_line = f.readline().strip('\n')
                print("Config file found.")
                splitted_line = first_line.split(':')
                if splitted_line[0] == 'workerThreadsNumber':
                    self.max_threads = int(splitted_line[1])
                    print("First line read.")
            except Exception as e:
                print(f"Error reading config: {e}")
        else:
            self.create_default_config()
    
    def create_default_config(self):
        """Create default configuration file."""
        try:
            os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
            with open(self.config_path, 'w') as f:
                f.write("workerThreadsNumber:20")
            print("Config file created.")
        except Exception as e:
            print(f"Error creating config: {e}")
    
    def save_config(self):
        """Save current configuration to file."""
        try:
            os.makedirs(os.path.dirname(self.config_path), exist_ok=True)
            with open(self.config_path, 'w') as f:
                f.write(f"workerThreadsNumber:{self.max_threads}")
            print("Config saved.")
        except Exception as e:
            print(f"Error saving config: {e}")


# Note: Global config instance removed in favor of dependency injection.
# Config is now instantiated in AppContext. See src/context.py

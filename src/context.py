"""
Application context for dependency injection.
Manages application-wide state without global singletons.
"""
from .data_models import StatisticsData, GUIModels
from .config import Config


class AppContext:
    """
    Central application context that holds all shared state.
    This replaces global singletons with dependency injection.
    """
    
    def __init__(self):
        """Initialize application context with all required components."""
        self.config = Config()
        self.stats_data = StatisticsData()
        self.gui_models = GUIModels()
    
    def reset_stats(self):
        """Reset statistics data."""
        self.stats_data.reset()
    
    def reset_gui_models(self):
        """Reset GUI models."""
        self.gui_models.clear_all()
    
    def reset_all(self):
        """Reset all application state."""
        self.reset_stats()
        self.reset_gui_models()
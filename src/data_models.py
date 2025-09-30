"""
Data models and structures.
Manages statistics data and GUI models.
"""
import threading
from collections import defaultdict
from PyQt6.QtGui import QStandardItemModel, QStandardItem


class StatisticsData:
    """Manages all statistics data for a user's film analysis."""
    
    def __init__(self):
        self.lock = threading.Lock()
        self.reset()
    
    def reset(self):
        """Reset all statistics data."""
        with self.lock:
            self.url_list = []
            self.url_set = set()
            self.lang_dict = {}
            self.country_dict = {}
            self.genre_dict = {}
            self.director_dict = {}
            self.actor_dict = {}
            self.decade_dict = defaultdict(int)
            
            # GUI display strings
            self.gui_watched1 = ""
            self.gui_watched2 = ""
            self.gui_lang = ""
            self.gui_lang_list = []
            self.gui_countries = ""
            self.gui_decades = ""
            self.gui_scraped_at = ""
            
            # Meta values for saving
            self.films_count = 0
            self.total_hours = 0.0
            self.total_days = 0.0
    
    def add_film_data(self, film_languages, film_countries, film_genres, 
                     film_directors, film_actors, decade):
        """Add data from a single film to the statistics."""
        with self.lock:
            if film_languages:
                for lang in film_languages:
                    self.lang_dict[lang] = self.lang_dict.get(lang, 0) + 1
            
            if film_countries:
                for country in film_countries:
                    self.country_dict[country] = self.country_dict.get(country, 0) + 1
            
            if film_genres:
                for genre in film_genres:
                    self.genre_dict[genre] = self.genre_dict.get(genre, 0) + 1
            
            if film_directors:
                for director in film_directors:
                    self.director_dict[director] = self.director_dict.get(director, 0) + 1
            
            if film_actors:
                for actor in film_actors:
                    self.actor_dict[actor] = self.actor_dict.get(actor, 0) + 1
            
            if decade:
                self.decade_dict[decade] += 1
    
    def add_url(self, url):
        """Add a film URL to the list if not already present."""
        with self.lock:
            if url not in self.url_set:
                self.url_set.add(url)
                self.url_list.append(url)
    
    def set_meta_data(self, films_count, total_hours, total_days, scraped_at):
        """Set meta information about the analysis."""
        with self.lock:
            self.films_count = films_count
            self.total_hours = total_hours
            self.total_days = total_days
            self.gui_scraped_at = scraped_at


class GUIModels:
    """Manages Qt models for displaying statistics in tables."""
    
    def __init__(self):
        self.models = {
            'countries': QStandardItemModel(0, 3),
            'languages': QStandardItemModel(0, 3),
            'genres': QStandardItemModel(0, 3),
            'directors': QStandardItemModel(0, 3),
            'actors': QStandardItemModel(0, 3)
        }
        
        # Set headers
        headers = ['Name', 'Films', 'Percentage']
        for model in self.models.values():
            model.setHorizontalHeaderLabels(headers)
    
    def clear_all(self):
        """Clear all models."""
        for model in self.models.values():
            model.removeRows(0, model.rowCount())
    
    def populate_model(self, model_name, data_dict, films_count, limit=None):
        """Populate a specific model with sorted data."""
        if model_name not in self.models:
            return
        
        model = self.models[model_name]
        model.removeRows(0, model.rowCount())
        
        sorted_data = dict(sorted(data_dict.items(), key=lambda x: x[1], reverse=True))
        
        count = 0
        for name, count_value in sorted_data.items():
            if limit and count >= limit:
                break
            
            percent = (format(count_value / films_count * 100, ".2f") + "%") if films_count else "0.00%"
            model.appendRow([
                QStandardItem(name),
                QStandardItem(str(count_value)),
                QStandardItem(percent)
            ])
            count += 1
    
    def get_model(self, name):
        """Get a specific model by name."""
        return self.models.get(name)

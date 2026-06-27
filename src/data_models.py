"""
Data models and structures.
Manages statistics data and GUI models.
"""
from collections import defaultdict


class StatisticsData:
    """Manages all statistics data for a user's film analysis."""
    
    def __init__(self):
        self.reset()
    
    def reset(self):
        """Reset all statistics data."""
        self.url_list = []
        self.url_set = set()
        self.lang_dict = {}
        self.country_dict = {}
        self.genre_dict = {}
        self.director_dict = {}
        self.actor_dict = {}
        self.decade_dict = defaultdict(int)
        
        # Diary analytics (multi-mode aggregation)
        self.diary_weekday_counts = {}   # {"Monday": 120, "Tuesday": 110, ...}
        self.diary_month_counts = {}     # {"January": 75, "February": 60, ...} (year-independent, exactly 12 keys)
        self.diary_year_counts = {}      # {"2024": 200, "2023": 180, ...}
        
        # Financial analytics (per-film ranking)
        self.film_budget_data = {}       # {"Film Name": budget_amount, ...}
        self.film_boxoffice_data = {}    # {"Film Name": box_office, ...}
        
        # Budget range buckets (distribution view)
        self.budget_range_buckets = {}   # {"buckets": [...], "totalFilmsWithBudget": N}
        
        # Watchlist analytics (separate dataset, mirrors watched fields above)
        self.wl_films_count = 0
        self.wl_total_hours = 0.0
        self.wl_lang_dict = {}
        self.wl_country_dict = {}
        self.wl_genre_dict = {}
        self.wl_director_dict = {}
        self.wl_actor_dict = {}
        self.wl_decade_dict = defaultdict(int)
        self.wl_film_budget_data = {}
        self.wl_film_boxoffice_data = {}
        self.wl_budget_range_buckets = {}  # {"buckets": [...], "totalFilmsWithBudget": N}
        
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
        if url not in self.url_set:
            self.url_set.add(url)
            self.url_list.append(url)
    
    def set_meta_data(self, films_count, total_hours, total_days, scraped_at):
        """Set meta information about the analysis."""
        self.films_count = films_count
        self.total_hours = total_hours
        self.total_days = total_days
        self.gui_scraped_at = scraped_at


class GUIModels:
    """Manages data models for displaying statistics.
    
    Uses plain Python dicts (compatible with WebUI JSON consumption).
    Previously used Qt models; now stores sorted list-of-dicts for
    each category, matching the old table structure:
        [{'name': str, 'count': int, 'percent': str}, ...]
    """
    
    def __init__(self):
        self.models = {
            'countries': [],
            'languages': [],
            'genres': [],
            'directors': [],
            'actors': []
        }
    
    def clear_all(self):
        """Clear all models."""
        for name in self.models:
            self.models[name] = []
    
    def populate_model(self, model_name, data_dict, films_count, limit=None):
        """Populate a specific model with sorted data."""
        if model_name not in self.models:
            return
        
        sorted_data = dict(sorted(data_dict.items(), key=lambda x: x[1], reverse=True))
        
        rows = []
        for name, count_value in sorted_data.items():
            if limit and len(rows) >= limit:
                break
            
            percent = f"{count_value / films_count * 100:.2f}%" if films_count else "0.00%"
            rows.append({
                'name': name,
                'count': count_value,
                'percent': percent
            })
        
        self.models[model_name] = rows
    
    def get_model(self, name):
        """Get a specific model by name."""
        return self.models.get(name)
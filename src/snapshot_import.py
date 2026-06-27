"""
Snapshot import functionality for LePrAn.

Imports a JSON snapshot file and restores the complete application state.
"""
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, Tuple

from .snapshot import (
    ApplicationSnapshot,
    FilmRecord,
    DiaryEntry,
    AnalyticsData,
    WatchlistData,
    SNAPSHOT_SCHEMA_VERSION
)
from .data_models import StatisticsData
from .data_models import GUIModels
from .analytics_utils import compute_budget_range_buckets

logger = logging.getLogger(__name__)



class SnapshotImportError(Exception):
    """Raised when snapshot import fails."""
    pass


class SnapshotImporter:
    """Imports a JSON snapshot file and restores the complete application state."""
    
    # Supported schema versions
    SUPPORTED_VERSIONS = ["1.0.0"]
    
    def __init__(self, stats_data: StatisticsData, gui_models: GUIModels):
        """
        Initialize the importer.
        
        Args:
            stats_data: The StatisticsData instance to populate
            gui_models: The GUIModels instance to populate
        """
        self.stats_data = stats_data
        self.gui_models = gui_models
    
    def validate_snapshot(self, snapshot: ApplicationSnapshot) -> Tuple[bool, str]:
        """
        Validate a snapshot for import.
        
        Args:
            snapshot: The ApplicationSnapshot to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        # Check schema version
        if snapshot.schema_version not in self.SUPPORTED_VERSIONS:
            return False, (
                f"Unsupported schema version: {snapshot.schema_version}. "
                f"Supported versions: {', '.join(self.SUPPORTED_VERSIONS)}"
            )
        
        # Check for required data
        if not snapshot.analytics and not snapshot.films:
            return False, "Snapshot contains no data (no analytics or films)"
        
        # Check analytics integrity
        analytics = snapshot.analytics
        if analytics.total_films == 0 and len(snapshot.films) == 0:
            return False, "Snapshot contains no films"
        
        logger.info("Snapshot validation passed")
        return True, ""
    
    def restore_from_snapshot(
        self,
        snapshot: ApplicationSnapshot,
        rebuild_from_films: bool = True
    ) -> dict:
        """
        Restore application state from a snapshot.
        
        Args:
            snapshot: The ApplicationSnapshot to restore from
            rebuild_from_films: If True, rebuild stats from individual film records.
                              If False, use pre-computed analytics directly.
                              
        Returns:
            Dict with restoration results including counts and warnings
            
        Raises:
            SnapshotImportError: If restoration fails
        """
        try:
            # Validate first
            is_valid, error_msg = self.validate_snapshot(snapshot)
            if not is_valid:
                raise SnapshotImportError(error_msg)
            
            # Reset current state
            self.stats_data.reset()
            self.gui_models.clear_all()
            
            # Extract username from analytics
            username = snapshot.analytics.username if snapshot.analytics else ""
            
            if rebuild_from_films:
                # Rebuild statistics from individual film records
                self._rebuild_from_films(snapshot)
                result = self._restore_from_rebuilt(snapshot)
            else:
                # Use pre-computed analytics directly
                result = self._restore_from_analytics(snapshot)
            
            # Add username to result
            result['username'] = username
            
            logger.info(f"Successfully restored application state: {result.get('films_restored', 0)} films")
            return result
            
        except SnapshotImportError:
            raise
        except Exception as e:
            logger.error(f"Failed to restore from snapshot: {e}")
            raise SnapshotImportError(f"Import failed: {str(e)}")
    
    @staticmethod
    def _normalized_film_key(title, year) -> str:
        return f"{str(title).strip().lower()}|{str(year).strip()}"
    
    def _snapshot_main_film_keys(self, snapshot: ApplicationSnapshot) -> set[str]:
        """Return normalized keys for films that belong to watched/diary analytics."""
        keys = set()
        for entry in snapshot.watched_entries or []:
            keys.add(self._normalized_film_key(entry.get('title', ''), entry.get('year', '')))
        for entry in snapshot.diary_entries or []:
            keys.add(self._normalized_film_key(entry.title, entry.year))
        return keys
    
    def _snapshot_watchlist_film_keys(self, snapshot: ApplicationSnapshot) -> set[str]:
        keys = set()
        for entry in snapshot.watchlist.films or []:
            keys.add(self._normalized_film_key(entry.get('title', ''), entry.get('year', '')))
        return keys
    
    def _is_main_film(self, key: str, film: FilmRecord, main_keys: set[str]) -> bool:
        normalized = self._normalized_film_key(film.title, film.year)
        if main_keys:
            return normalized in main_keys
        return not film.in_watchlist
    
    def _rebuild_from_films(self, snapshot: ApplicationSnapshot):
        """Rebuild watched/diary statistics from individual film records."""
        films = snapshot.films
        main_keys = self._snapshot_main_film_keys(snapshot)
        main_films = {}
        
        for key, film in films.items():
            if not self._is_main_film(key, film, main_keys):
                continue
            main_films[key] = film
            
            # Add URL if available
            if film.tmdb_id:
                self.stats_data.add_url(f"tmdb://{film.tmdb_id}")
            
            # Add film data to watched/diary statistics only
            self.stats_data.add_film_data(
                film_languages=film.languages,
                film_countries=film.countries,
                film_genres=film.genres,
                film_directors=film.directors,
                film_actors=film.actors,
                decade=film.decade
            )
        
        watched_keys = {
            self._normalized_film_key(entry.get('title', ''), entry.get('year', ''))
            for entry in (snapshot.watched_entries or [])
        }
        self._rebuilt_total_films = len(watched_keys) if watched_keys else len(main_films)
        self._rebuilt_total_hours = sum(
            (film.runtime or 0) for film in main_films.values()
        ) / 60.0
        
        # Rebuild diary analytics from diary entries
        self._rebuild_diary_analytics(snapshot.diary_entries)
        
        # Rebuild financial analytics from watched/diary film records only
        self._rebuild_financial_analytics(main_films)
    
    def _rebuild_diary_analytics(self, diary_entries: list):
        """Rebuild diary analytics (weekday/month/year counts) from diary entries."""
        from datetime import datetime
        
        for entry in diary_entries:
            # Parse date and compute weekday
            try:
                dt = datetime.strptime(entry.date, "%Y-%m-%d")
                weekday = dt.strftime("%A")  # e.g., "Monday"
                self.stats_data.diary_weekday_counts[weekday] = \
                    self.stats_data.diary_weekday_counts.get(weekday, 0) + 1
                
                # Month key: month name (year-independent, e.g., "January")
                month_key = dt.strftime("%B")
                self.stats_data.diary_month_counts[month_key] = \
                    self.stats_data.diary_month_counts.get(month_key, 0) + 1
            except (ValueError, AttributeError):
                # Skip entries with invalid dates
                pass
            
            # Year key: YYYY (from entry date or film year)
            year_key = entry.date[:4] if len(entry.date) >= 4 else str(entry.year)
            self.stats_data.diary_year_counts[year_key] = \
                self.stats_data.diary_year_counts.get(year_key, 0) + 1
    
    def _rebuild_financial_analytics(self, films: dict):
        """Rebuild financial analytics (budget/boxoffice per-film ranking) from film records."""
        for key, film in films.items():
            # Budget data
            if film.budget is not None and film.budget > 0:
                self.stats_data.film_budget_data[film.title] = film.budget
            
            # Box office data
            if film.box_office is not None and film.box_office > 0:
                self.stats_data.film_boxoffice_data[film.title] = film.box_office
        
        # Compute budget range buckets from the rebuilt budget data
        budget_data = dict(self.stats_data.film_budget_data)
        self.stats_data.budget_range_buckets = compute_budget_range_buckets(budget_data)
    
    def _rebuild_watchlist_analytics(self, snapshot: ApplicationSnapshot):
        """Rebuild watchlist analytics from film records marked as in_watchlist."""
        analytics = snapshot.analytics
        films = snapshot.films
        
        # Collect watchlist films from explicit watchlist entries and record flags.
        watchlist_keys = self._snapshot_watchlist_film_keys(snapshot)
        wl_films = []
        for key, film in films.items():
            normalized = self._normalized_film_key(film.title, film.year)
            if film.in_watchlist or normalized in watchlist_keys:
                wl_films.append(film)
        
        # Set watchlist film count and hours
        self.stats_data.wl_films_count = len(wl_films)
        
        # Compute total hours for watchlist films
        total_wl_hours = 0.0
        for film in wl_films:
            if film.runtime is not None:
                total_wl_hours += film.runtime / 60.0
        
        self.stats_data.wl_total_hours = total_wl_hours
        
        # Compute watchlist analytics dictionaries
        wl_lang_dict = {}
        wl_country_dict = {}
        wl_genre_dict = {}
        wl_director_dict = {}
        wl_actor_dict = {}
        wl_decade_dict = {}
        wl_film_budget_data = {}
        wl_film_boxoffice_data = {}
        
        for film in wl_films:
            # Languages
            for lang in film.languages:
                wl_lang_dict[lang] = wl_lang_dict.get(lang, 0) + 1
            
            # Countries
            for country in film.countries:
                wl_country_dict[country] = wl_country_dict.get(country, 0) + 1
            
            # Genres
            for genre in film.genres:
                wl_genre_dict[genre] = wl_genre_dict.get(genre, 0) + 1
            
            # Directors
            for director in film.directors:
                wl_director_dict[director] = wl_director_dict.get(director, 0) + 1
            
            # Actors
            for actor in film.actors:
                wl_actor_dict[actor] = wl_actor_dict.get(actor, 0) + 1
            
            # Decades
            if film.decade:
                wl_decade_dict[film.decade] = wl_decade_dict.get(film.decade, 0) + 1
            
            # Budget data
            if film.budget is not None and film.budget > 0:
                wl_film_budget_data[film.title] = film.budget
            
            # Box office data
            if film.box_office is not None and film.box_office > 0:
                wl_film_boxoffice_data[film.title] = film.box_office
        
        # Update stats data
        self.stats_data.wl_lang_dict.clear()
        self.stats_data.wl_lang_dict.update(wl_lang_dict)
        self.stats_data.wl_country_dict.clear()
        self.stats_data.wl_country_dict.update(wl_country_dict)
        self.stats_data.wl_genre_dict.clear()
        self.stats_data.wl_genre_dict.update(wl_genre_dict)
        self.stats_data.wl_director_dict.clear()
        self.stats_data.wl_director_dict.update(wl_director_dict)
        self.stats_data.wl_actor_dict.clear()
        self.stats_data.wl_actor_dict.update(wl_actor_dict)
        self.stats_data.wl_decade_dict.clear()
        self.stats_data.wl_decade_dict.update(wl_decade_dict)
        self.stats_data.wl_film_budget_data.clear()
        self.stats_data.wl_film_budget_data.update(wl_film_budget_data)
        self.stats_data.wl_film_boxoffice_data.clear()
        self.stats_data.wl_film_boxoffice_data.update(wl_film_boxoffice_data)
        
        # Compute watchlist budget range buckets
        wl_budget_range = compute_budget_range_buckets(wl_film_budget_data)
        self.stats_data.wl_budget_range_buckets.clear()
        self.stats_data.wl_budget_range_buckets.update(wl_budget_range)
    
    def _restore_from_rebuilt(self, snapshot: ApplicationSnapshot) -> dict:
        """Restore state after rebuilding from film records."""
        analytics = snapshot.analytics
        
        rebuilt_total_films = getattr(self, '_rebuilt_total_films', analytics.total_films)
        rebuilt_total_hours = getattr(self, '_rebuilt_total_hours', analytics.total_hours)
        total_days = rebuilt_total_hours / 24.0
        
        # Set meta data from the rebuilt watched/diary dataset, not all film records.
        self.stats_data.set_meta_data(
            films_count=rebuilt_total_films,
            total_hours=rebuilt_total_hours,
            total_days=total_days,
            scraped_at=analytics.scraped_at
        )
        
        # Populate GUI models
        self.gui_models.populate_model('countries', self.stats_data.country_dict, rebuilt_total_films)
        self.gui_models.populate_model('languages', self.stats_data.lang_dict, rebuilt_total_films)
        self.gui_models.populate_model('genres', self.stats_data.genre_dict, rebuilt_total_films)
        self.gui_models.populate_model('directors', self.stats_data.director_dict, rebuilt_total_films)
        self.gui_models.populate_model('actors', self.stats_data.actor_dict, rebuilt_total_films)
        
        # Rebuild watchlist analytics from film records marked as in_watchlist
        self._rebuild_watchlist_analytics(snapshot)
        
        return {
            'films_restored': rebuilt_total_films,
            'diary_entries_restored': len(snapshot.diary_entries),
            'watchlist_items_restored': snapshot.watchlist.total_count,
            'method': 'rebuilt_from_films'
        }
    
    def _restore_from_analytics(self, snapshot: ApplicationSnapshot) -> dict:
        """Restore state directly from pre-computed analytics."""
        analytics = snapshot.analytics
        
        # Compute total_days from total_hours (single source of truth)
        total_days = analytics.total_hours / 24.0
        
        # Restore dictionaries directly
        self.stats_data.lang_dict.clear()
        self.stats_data.lang_dict.update(analytics.language_stats)
        
        self.stats_data.country_dict.clear()
        self.stats_data.country_dict.update(analytics.country_stats)
        
        self.stats_data.genre_dict.clear()
        self.stats_data.genre_dict.update(analytics.genre_stats)
        
        self.stats_data.director_dict.clear()
        self.stats_data.director_dict.update(analytics.director_stats)
        
        self.stats_data.actor_dict.clear()
        self.stats_data.actor_dict.update(analytics.actor_stats)
        
        self.stats_data.decade_dict.clear()
        self.stats_data.decade_dict.update(analytics.decade_stats)
        
        # Restore diary analytics from pre-computed stats
        if analytics.weekday_stats:
            self.stats_data.diary_weekday_counts.clear()
            self.stats_data.diary_weekday_counts.update(analytics.weekday_stats)
        
        if analytics.month_stats:
            self.stats_data.diary_month_counts.clear()
            self.stats_data.diary_month_counts.update(analytics.month_stats)
        
        if analytics.year_stats:
            self.stats_data.diary_year_counts.clear()
            self.stats_data.diary_year_counts.update(analytics.year_stats)
        
        # Restore financial analytics from pre-computed ranking
        if analytics.film_budget_ranking:
            self.stats_data.film_budget_data.clear()
            self.stats_data.film_budget_data.update(analytics.film_budget_ranking)
        
        if analytics.film_boxoffice_ranking:
            self.stats_data.film_boxoffice_data.clear()
            self.stats_data.film_boxoffice_data.update(analytics.film_boxoffice_ranking)
        
        # Restore budget range buckets (backward compatible: empty dict if missing)
        if analytics.budget_range_buckets:
            self.stats_data.budget_range_buckets.clear()
            self.stats_data.budget_range_buckets.update(analytics.budget_range_buckets)
        
        # Set meta data
        self.stats_data.set_meta_data(
            films_count=analytics.total_films,
            total_hours=analytics.total_hours,
            total_days=total_days,
            scraped_at=analytics.scraped_at
        )
        
        # Populate GUI models
        self.gui_models.populate_model('countries', self.stats_data.country_dict, analytics.total_films)
        self.gui_models.populate_model('languages', self.stats_data.lang_dict, analytics.total_films)
        self.gui_models.populate_model('genres', self.stats_data.genre_dict, analytics.total_films)
        self.gui_models.populate_model('directors', self.stats_data.director_dict, analytics.total_films)
        self.gui_models.populate_model('actors', self.stats_data.actor_dict, analytics.total_films)
        
        # Restore watchlist analytics from pre-computed analytics
        self._restore_watchlist_analytics(snapshot)
        
        return {
            'films_restored': analytics.total_films,
            'diary_entries_restored': len(snapshot.diary_entries),
            'watchlist_items_restored': snapshot.watchlist.total_count,
            'method': 'precomputed_analytics'
        }
    
    def _restore_watchlist_analytics(self, snapshot: ApplicationSnapshot):
        """Restore watchlist analytics from snapshot data."""
        analytics = snapshot.analytics
        
        # Restore watchlist film count and hours
        self.stats_data.wl_films_count = analytics.wl_total_films if hasattr(analytics, 'wl_total_films') else 0
        self.stats_data.wl_total_hours = analytics.wl_total_hours if hasattr(analytics, 'wl_total_hours') else 0.0
        
        # Restore watchlist dictionaries
        if hasattr(analytics, 'wl_language_stats') and analytics.wl_language_stats:
            self.stats_data.wl_lang_dict.clear()
            self.stats_data.wl_lang_dict.update(analytics.wl_language_stats)
        
        if hasattr(analytics, 'wl_country_stats') and analytics.wl_country_stats:
            self.stats_data.wl_country_dict.clear()
            self.stats_data.wl_country_dict.update(analytics.wl_country_stats)
        
        if hasattr(analytics, 'wl_genre_stats') and analytics.wl_genre_stats:
            self.stats_data.wl_genre_dict.clear()
            self.stats_data.wl_genre_dict.update(analytics.wl_genre_stats)
        
        if hasattr(analytics, 'wl_director_stats') and analytics.wl_director_stats:
            self.stats_data.wl_director_dict.clear()
            self.stats_data.wl_director_dict.update(analytics.wl_director_stats)
        
        if hasattr(analytics, 'wl_actor_stats') and analytics.wl_actor_stats:
            self.stats_data.wl_actor_dict.clear()
            self.stats_data.wl_actor_dict.update(analytics.wl_actor_stats)
        
        if hasattr(analytics, 'wl_decade_stats') and analytics.wl_decade_stats:
            self.stats_data.wl_decade_dict.clear()
            self.stats_data.wl_decade_dict.update(analytics.wl_decade_stats)
        
        # Restore watchlist financial analytics from pre-computed ranking
        if hasattr(analytics, 'wl_film_budget_ranking') and analytics.wl_film_budget_ranking:
            self.stats_data.wl_film_budget_data.clear()
            self.stats_data.wl_film_budget_data.update(analytics.wl_film_budget_ranking)
        
        if hasattr(analytics, 'wl_film_boxoffice_ranking') and analytics.wl_film_boxoffice_ranking:
            self.stats_data.wl_film_boxoffice_data.clear()
            self.stats_data.wl_film_boxoffice_data.update(analytics.wl_film_boxoffice_ranking)
        
        # Restore watchlist budget range buckets
        if hasattr(analytics, 'wl_budget_range_buckets') and analytics.wl_budget_range_buckets:
            self.stats_data.wl_budget_range_buckets.clear()
            self.stats_data.wl_budget_range_buckets.update(analytics.wl_budget_range_buckets)
    
    def import_from_file(self, file_path: str, rebuild_from_films: bool = True) -> dict:
        """
        Import and restore from a snapshot file.
        
        Args:
            file_path: Path to the snapshot JSON file
            rebuild_from_films: Whether to rebuild stats from film records
            
        Returns:
            Dict with restoration results
            
        Raises:
            SnapshotImportError: If import fails
        """
        try:
            path = Path(file_path)
            
            if not path.exists():
                raise SnapshotImportError(f"File not found: {file_path}")
            
            if not path.is_file():
                raise SnapshotImportError(f"Path is not a file: {file_path}")
            
            # Read and parse JSON
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Create snapshot object
            snapshot = ApplicationSnapshot.from_dict(data)
            
            # Restore state
            return self.restore_from_snapshot(snapshot, rebuild_from_films)
            
        except json.JSONDecodeError as e:
            raise SnapshotImportError(f"Invalid JSON format: {e}")
        except SnapshotImportError:
            raise
        except Exception as e:
            raise SnapshotImportError(f"Import failed: {str(e)}")
    
    def import_from_json(self, json_str: str, rebuild_from_films: bool = True) -> dict:
        """
        Import and restore from a JSON string.
        
        Args:
            json_str: JSON string containing the snapshot
            rebuild_from_films: Whether to rebuild stats from film records
            
        Returns:
            Dict with restoration results
            
        Raises:
            SnapshotImportError: If import fails
        """
        try:
            # Parse JSON
            data = json.loads(json_str)
            
            # Create snapshot object
            snapshot = ApplicationSnapshot.from_dict(data)
            
            # Restore state
            return self.restore_from_snapshot(snapshot, rebuild_from_films)
            
        except json.JSONDecodeError as e:
            raise SnapshotImportError(f"Invalid JSON format: {e}")
        except SnapshotImportError:
            raise
        except Exception as e:
            raise SnapshotImportError(f"Import failed: {str(e)}")
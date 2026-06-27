"""
Snapshot export functionality for LePrAn.

Exports the complete application state to a JSON file that can be
used to fully reconstruct the application at a later time.
"""
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional

from .snapshot import (
    ApplicationSnapshot,
    FilmRecord,
    DiaryEntry,
    AnalyticsData,
    WatchlistData,
    SNAPSHOT_SCHEMA_VERSION
)
from .data_models import StatisticsData

logger = logging.getLogger(__name__)


class SnapshotExporter:
    """Exports the complete LePrAn application state to a JSON snapshot."""
    
    def __init__(self, stats_data: StatisticsData, login_input: str = ""):
        """
        Initialize the exporter.
        
        Args:
            stats_data: The StatisticsData instance containing current analytics
            login_input: The username/identifier
        """
        self.stats_data = stats_data
        self.login_input = login_input
    
    def create_snapshot_from_stats(
        self,
        films: Optional[dict] = None,
        diary_entries: Optional[list] = None,
        watched_entries: Optional[list] = None,
        watchlist_data: Optional[list] = None,
        raw_watched_csv: str = "",
        raw_diary_csv: str = "",
        raw_watchlist_csv: str = "",
        analytics: Optional[dict] = None
    ) -> ApplicationSnapshot:
        """
        Create a snapshot from the current statistics data and film records.
        
        Args:
            films: Dict of film records keyed by "title|year"
            diary_entries: List of diary entry dicts
            watched_entries: List of watched entry dicts
            watchlist_data: List of watchlist entry dicts (title, year, date, uri) or None
            raw_watched_csv: Raw watched.csv content
            raw_diary_csv: Raw diary.csv content
            raw_watchlist_csv: Raw watchlist.csv content
            analytics: Optional dict of pre-built analytics data to override auto-computed values
            
        Returns:
            ApplicationSnapshot with all available data
        """
        if analytics is not None:
            # Use provided analytics dict
            analytics_obj = AnalyticsData(
                total_films=analytics.get('total_films', self.stats_data.films_count),
                total_hours=analytics.get('total_hours', self.stats_data.total_hours),
                # total_days is derived from total_hours (days = hours / 24) - NOT stored
                language_stats=analytics.get('language_stats', dict(self.stats_data.lang_dict)),
                country_stats=analytics.get('country_stats', dict(self.stats_data.country_dict)),
                genre_stats=analytics.get('genre_stats', dict(self.stats_data.genre_dict)),
                director_stats=analytics.get('director_stats', dict(self.stats_data.director_dict)),
                actor_stats=analytics.get('actor_stats', dict(self.stats_data.actor_dict)),
                decade_stats=analytics.get('decade_stats', dict(self.stats_data.decade_dict)),
                weekday_stats=analytics.get('weekday_stats', dict(self.stats_data.diary_weekday_counts)),
                month_stats=analytics.get('month_stats', dict(self.stats_data.diary_month_counts)),
                year_stats=analytics.get('year_stats', dict(self.stats_data.diary_year_counts)),
                rating_stats=analytics.get('rating_stats', {}),
                tag_stats=analytics.get('tag_stats', {}),
                total_budget=analytics.get('total_budget'),
                total_box_office=analytics.get('total_box_office'),
                avg_budget=analytics.get('avg_budget'),
                avg_runtime=analytics.get('avg_runtime'),
                film_budget_ranking=analytics.get('film_budget_ranking', dict(self.stats_data.film_budget_data)),
                film_boxoffice_ranking=analytics.get('film_boxoffice_ranking', dict(self.stats_data.film_boxoffice_data)),
                budget_range_buckets=analytics.get('budget_range_buckets', {}),
                username=analytics.get('username', self.login_input),
                scraped_at=analytics.get('scraped_at', self.stats_data.gui_scraped_at or datetime.now().strftime("%Y-%m-%d"))
            )
        else:
            # Build analytics from stats_data
            # total_days is derived from total_hours (days = hours / 24) - NOT stored
            analytics_obj = AnalyticsData(
                total_films=self.stats_data.films_count,
                total_hours=self.stats_data.total_hours,
                # total_days is derived from total_hours (days = hours / 24) - NOT stored
                language_stats=dict(self.stats_data.lang_dict),
                country_stats=dict(self.stats_data.country_dict),
                genre_stats=dict(self.stats_data.genre_dict),
                director_stats=dict(self.stats_data.director_dict),
                actor_stats=dict(self.stats_data.actor_dict),
                decade_stats=dict(self.stats_data.decade_dict),
                weekday_stats=dict(self.stats_data.diary_weekday_counts),
                month_stats=dict(self.stats_data.diary_month_counts),
                year_stats=dict(self.stats_data.diary_year_counts),
                film_budget_ranking=dict(self.stats_data.film_budget_data),
                film_boxoffice_ranking=dict(self.stats_data.film_boxoffice_data),
                budget_range_buckets={},
                username=self.login_input,
                scraped_at=self.stats_data.gui_scraped_at or datetime.now().strftime("%Y-%m-%d")
            )
        
        # Convert watchlist_data list to WatchlistData object
        watchlist = WatchlistData()
        if watchlist_data:
            watchlist.films = watchlist_data
            watchlist.total_count = len(watchlist_data)
        
        # Convert plain film dicts to FilmRecord objects
        film_records = {}
        for key, film_dict in (films or {}).items():
            if isinstance(film_dict, FilmRecord):
                film_records[key] = film_dict
            else:
                # Convert dict to FilmRecord
                film_records[key] = FilmRecord(
                    title=film_dict.get('title', ''),
                    year=film_dict.get('year', 0),
                    tmdb_id=film_dict.get('tmdb_id'),
                    languages=film_dict.get('languages', []),
                    countries=film_dict.get('countries', []),
                    genres=film_dict.get('genres', []),
                    directors=film_dict.get('directors', []),
                    actors=film_dict.get('actors', []),
                    decade=film_dict.get('decade'),
                    runtime=film_dict.get('runtime'),
                    budget=film_dict.get('budget'),
                    box_office=film_dict.get('box_office'),
                    overview=film_dict.get('overview', ''),
                    poster_path=film_dict.get('poster_path', ''),
                    in_watchlist=film_dict.get('in_watchlist', False),
                    added_to_watchlist_date=film_dict.get('added_to_watchlist_date'),
                    date_added_to_library=film_dict.get('date_added_to_library'),
                    times_watched=film_dict.get('times_watched', 0),
                    average_rating=film_dict.get('average_rating')
                )
        
        # Convert plain diary entry dicts to DiaryEntry objects
        diary_entry_objects = []
        for entry_dict in (diary_entries or []):
            if isinstance(entry_dict, DiaryEntry):
                diary_entry_objects.append(entry_dict)
            else:
                diary_entry_objects.append(DiaryEntry(
                    date=entry_dict.get('date', ''),
                    title=entry_dict.get('title', ''),
                    year=entry_dict.get('year', 0),
                    rating=entry_dict.get('rating'),
                    tags=entry_dict.get('tags', []),
                    notes=entry_dict.get('notes', ''),
                    tmdb_id=entry_dict.get('tmdb_id')
                ))
        
        # Create snapshot with properly typed objects
        snapshot = ApplicationSnapshot(
            schema_version=SNAPSHOT_SCHEMA_VERSION,
            export_date=datetime.now().strftime("%Y-%m-%d"),
            films=film_records,
            diary_entries=diary_entry_objects,
            watched_entries=watched_entries or [],
            watchlist=watchlist,
            analytics=analytics_obj,
            raw_watched_csv=raw_watched_csv,
            raw_diary_csv=raw_diary_csv,
            raw_watchlist_csv=raw_watchlist_csv
        )
        
        logger.info(f"Created snapshot with {len(snapshot.films)} films, "
                   f"{len(snapshot.diary_entries)} diary entries")
        
        return snapshot
    
    def export_to_file(
        self,
        snapshot: ApplicationSnapshot,
        output_path: str
    ) -> bool:
        """
        Export snapshot to a JSON file.
        
        Args:
            snapshot: The ApplicationSnapshot to export
            output_path: Path to write the JSON file
            
        Returns:
            True if successful, False otherwise
        """
        try:
            output_path = Path(output_path)
            
            # Ensure parent directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write JSON
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(snapshot.to_json(indent=2))
            
            logger.info(f"Successfully exported snapshot to {output_path}")
            return True
            
        except IOError as e:
            logger.error(f"Failed to export snapshot to {output_path}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error exporting snapshot: {e}")
            return False
    
    def export_stats_to_file(
        self,
        output_path: str,
        films: Optional[dict] = None,
        diary_entries: Optional[list] = None,
        watched_entries: Optional[list] = None,
        watchlist_data: Optional[dict] = None,
        raw_watched_csv: str = "",
        raw_diary_csv: str = "",
        raw_watchlist_csv: str = "",
        analytics: Optional[dict] = None
    ) -> bool:
        """
        Create and export a snapshot in one step.
        
        Args:
            output_path: Path to write the JSON file
            films: Dict of film records
            diary_entries: List of diary entries
            watched_entries: List of watched entries
            watchlist_data: Watchlist data
            raw_watched_csv: Raw watched.csv content
            raw_diary_csv: Raw diary.csv content
            raw_watchlist_csv: Raw watchlist.csv content
            analytics: Optional dict of pre-built analytics data
            
        Returns:
            True if successful, False otherwise
        """
        snapshot = self.create_snapshot_from_stats(
            films=films,
            diary_entries=diary_entries,
            watched_entries=watched_entries,
            watchlist_data=watchlist_data,
            raw_watched_csv=raw_watched_csv,
            raw_diary_csv=raw_diary_csv,
            raw_watchlist_csv=raw_watchlist_csv,
            analytics=analytics
        )
        
        return self.export_to_file(snapshot, output_path)

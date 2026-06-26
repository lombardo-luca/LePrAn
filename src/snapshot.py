"""
Snapshot data structures for complete application state persistence.

The snapshot system captures a complete state of the LePrAn application
including:
- Film library data (from TMDB scraping)
- Diary entries (date watched, rating, tags, notes)
- Watched data
- Watchlist data
- Computed analytics (decade, country, language, genre, actor, director, weekday, budget, box office)
"""
import json
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from datetime import datetime


# Current snapshot schema version
SNAPSHOT_SCHEMA_VERSION = "1.0.0"


@dataclass
class DiaryEntry:
    """A single diary entry with all metadata."""
    date: str  # Format: YYYY-MM-DD
    title: str
    year: int
    rating: Optional[int] = None  # 1-5 stars
    tags: List[str] = field(default_factory=list)
    notes: str = ""
    tmdb_id: Optional[int] = None


@dataclass
class FilmRecord:
    """Complete record for a single film with all available metadata."""
    title: str
    year: int
    tmdb_id: Optional[int] = None
    
    # TMDB-derived metadata
    languages: List[str] = field(default_factory=list)
    countries: List[str] = field(default_factory=list)
    genres: List[str] = field(default_factory=list)
    directors: List[str] = field(default_factory=list)
    actors: List[str] = field(default_factory=list)
    decade: Optional[str] = None
    runtime: Optional[int] = None
    budget: Optional[float] = None
    box_office: Optional[float] = None
    overview: str = ""
    poster_path: str = ""
    
    # Diary entries for this film (can have multiple viewings)
    diary_entries: List[DiaryEntry] = field(default_factory=list)
    
    # Watchlist status
    in_watchlist: bool = False
    added_to_watchlist_date: Optional[str] = None  # YYYY-MM-DD
    
    # Computed fields
    date_added_to_library: Optional[str] = None  # YYYY-MM-DD
    times_watched: int = 0
    average_rating: Optional[float] = None


@dataclass
class AnalyticsData:
    """Pre-computed analytics that can be derived from the film library."""
    
    # Basic counts
    total_films: int = 0
    total_hours: float = 0.0
    total_days: float = 0.0
    
    # Category statistics
    language_stats: Dict[str, int] = field(default_factory=dict)
    country_stats: Dict[str, int] = field(default_factory=dict)
    genre_stats: Dict[str, int] = field(default_factory=dict)
    director_stats: Dict[str, int] = field(default_factory=dict)
    actor_stats: Dict[str, int] = field(default_factory=dict)
    decade_stats: Dict[str, int] = field(default_factory=dict)
    
    # Diary-specific statistics
    weekday_stats: Dict[str, int] = field(default_factory=dict)  # e.g., {"Monday": 5, "Tuesday": 3, ...}
    rating_stats: Dict[str, int] = field(default_factory=dict)  # e.g., {"5": 10, "4": 5, ...}
    tag_stats: Dict[str, int] = field(default_factory=dict)
    
    # Financial statistics
    total_budget: Optional[float] = None
    total_box_office: Optional[float] = None
    avg_budget: Optional[float] = None
    avg_runtime: Optional[float] = None
    
    # Metadata
    username: str = ""
    scraped_at: str = ""  # YYYY-MM-DD
    snapshot_version: str = SNAPSHOT_SCHEMA_VERSION


@dataclass
class WatchlistData:
    """Watchlist information."""
    films: List[dict] = field(default_factory=list)  # List of {title, year, added_date}
    total_count: int = 0


@dataclass
class ApplicationSnapshot:
    """Complete application state snapshot."""
    
    # Schema version
    schema_version: str = SNAPSHOT_SCHEMA_VERSION
    export_date: str = ""  # YYYY-MM-DD
    export_tool_version: str = "LePrAn Snapshot v1.0"
    
    # Film library
    films: Dict[str, FilmRecord] = field(default_factory=dict)  # Key: "title|year" normalized
    
    # Diary entries (flattened for easy access)
    diary_entries: List[DiaryEntry] = field(default_factory=list)
    
    # Watched entries (films that were watched but not in diary)
    watched_entries: List[dict] = field(default_factory=list)  # {title, year, date}
    
    # Watchlist
    watchlist: WatchlistData = field(default_factory=WatchlistData)
    
    # Pre-computed analytics
    analytics: AnalyticsData = field(default_factory=AnalyticsData)
    
    # Raw CSV data (preserved for exact reconstruction)
    raw_watched_csv: str = ""
    raw_diary_csv: str = ""
    raw_watchlist_csv: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert snapshot to dictionary for JSON serialization."""
        films_dict = {}
        for key, film in self.films.items():
            films_dict[key] = {
                'title': film.title,
                'year': film.year,
                'tmdb_id': film.tmdb_id,
                'languages': film.languages,
                'countries': film.countries,
                'genres': film.genres,
                'directors': film.directors,
                'actors': film.actors,
                'decade': film.decade,
                'runtime': film.runtime,
                'budget': film.budget,
                'box_office': film.box_office,
                'overview': film.overview,
                'poster_path': film.poster_path,
                'diary_entries': [
                    {
                        'date': e.date,
                        'title': e.title,
                        'year': e.year,
                        'rating': e.rating,
                        'tags': e.tags,
                        'notes': e.notes,
                        'tmdb_id': e.tmdb_id
                    }
                    for e in film.diary_entries
                ],
                'in_watchlist': film.in_watchlist,
                'added_to_watchlist_date': film.added_to_watchlist_date,
                'date_added_to_library': film.date_added_to_library,
                'times_watched': film.times_watched,
                'average_rating': film.average_rating
            }
        
        return {
            'schema_version': self.schema_version,
            'export_date': self.export_date,
            'export_tool_version': self.export_tool_version,
            'films': films_dict,
            'diary_entries': [
                {
                    'date': e.date,
                    'title': e.title,
                    'year': e.year,
                    'rating': e.rating,
                    'tags': e.tags,
                    'notes': e.notes,
                    'tmdb_id': e.tmdb_id
                }
                for e in self.diary_entries
            ],
            'watched_entries': self.watched_entries,
            'watchlist': {
                'films': self.watchlist.films,
                'total_count': self.watchlist.total_count
            },
            'analytics': {
                'total_films': self.analytics.total_films,
                'total_hours': self.analytics.total_hours,
                'total_days': self.analytics.total_days,
                'language_stats': self.analytics.language_stats,
                'country_stats': self.analytics.country_stats,
                'genre_stats': self.analytics.genre_stats,
                'director_stats': self.analytics.director_stats,
                'actor_stats': self.analytics.actor_stats,
                'decade_stats': self.analytics.decade_stats,
                'weekday_stats': self.analytics.weekday_stats,
                'rating_stats': self.analytics.rating_stats,
                'tag_stats': self.analytics.tag_stats,
                'total_budget': self.analytics.total_budget,
                'total_box_office': self.analytics.total_box_office,
                'avg_budget': self.analytics.avg_budget,
                'avg_runtime': self.analytics.avg_runtime,
                'username': self.analytics.username,
                'scraped_at': self.analytics.scraped_at,
                'snapshot_version': self.analytics.snapshot_version
            },
            'raw_csv_data': {
                'watched': self.raw_watched_csv,
                'diary': self.raw_diary_csv,
                'watchlist': self.raw_watchlist_csv
            }
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ApplicationSnapshot':
        """Create snapshot from dictionary (deserialization)."""
        snapshot = cls(
            schema_version=data.get('schema_version', SNAPSHOT_SCHEMA_VERSION),
            export_date=data.get('export_date', ''),
            export_tool_version=data.get('export_tool_version', 'Unknown'),
            raw_watched_csv=data.get('raw_csv_data', {}).get('watched', ''),
            raw_diary_csv=data.get('raw_csv_data', {}).get('diary', ''),
            raw_watchlist_csv=data.get('raw_csv_data', {}).get('watchlist', '')
        )
        
        # Load films
        films_data = data.get('films', {})
        for key, film_data in films_data.items():
            film = FilmRecord(
                title=film_data.get('title', ''),
                year=film_data.get('year', 0),
                tmdb_id=film_data.get('tmdb_id'),
                languages=film_data.get('languages', []),
                countries=film_data.get('countries', []),
                genres=film_data.get('genres', []),
                directors=film_data.get('directors', []),
                actors=film_data.get('actors', []),
                decade=film_data.get('decade'),
                runtime=film_data.get('runtime'),
                budget=film_data.get('budget'),
                box_office=film_data.get('box_office'),
                overview=film_data.get('overview', ''),
                poster_path=film_data.get('poster_path', ''),
                in_watchlist=film_data.get('in_watchlist', False),
                added_to_watchlist_date=film_data.get('added_to_watchlist_date'),
                date_added_to_library=film_data.get('date_added_to_library'),
                times_watched=film_data.get('times_watched', 0),
                average_rating=film_data.get('average_rating')
            )
            
            # Load diary entries
            for entry_data in film_data.get('diary_entries', []):
                entry = DiaryEntry(
                    date=entry_data.get('date', ''),
                    title=entry_data.get('title', ''),
                    year=entry_data.get('year', 0),
                    rating=entry_data.get('rating'),
                    tags=entry_data.get('tags', []),
                    notes=entry_data.get('notes', ''),
                    tmdb_id=entry_data.get('tmdb_id')
                )
                film.diary_entries.append(entry)
            
            snapshot.films[key] = film
        
        # Load diary entries (flattened)
        for entry_data in data.get('diary_entries', []):
            entry = DiaryEntry(
                date=entry_data.get('date', ''),
                title=entry_data.get('title', ''),
                year=entry_data.get('year', 0),
                rating=entry_data.get('rating'),
                tags=entry_data.get('tags', []),
                notes=entry_data.get('notes', ''),
                tmdb_id=entry_data.get('tmdb_id')
            )
            snapshot.diary_entries.append(entry)
        
        # Load watched entries
        snapshot.watched_entries = data.get('watched_entries', [])
        
        # Load watchlist
        watchlist_data = data.get('watchlist', {})
        snapshot.watchlist = WatchlistData(
            films=watchlist_data.get('films', []),
            total_count=watchlist_data.get('total_count', 0)
        )
        
        # Load analytics
        analytics_data = data.get('analytics', {})
        snapshot.analytics = AnalyticsData(
            total_films=analytics_data.get('total_films', 0),
            total_hours=analytics_data.get('total_hours', 0.0),
            total_days=analytics_data.get('total_days', 0.0),
            language_stats=analytics_data.get('language_stats', {}),
            country_stats=analytics_data.get('country_stats', {}),
            genre_stats=analytics_data.get('genre_stats', {}),
            director_stats=analytics_data.get('director_stats', {}),
            actor_stats=analytics_data.get('actor_stats', {}),
            decade_stats=analytics_data.get('decade_stats', {}),
            weekday_stats=analytics_data.get('weekday_stats', {}),
            rating_stats=analytics_data.get('rating_stats', {}),
            tag_stats=analytics_data.get('tag_stats', {}),
            total_budget=analytics_data.get('total_budget'),
            total_box_office=analytics_data.get('total_box_office'),
            avg_budget=analytics_data.get('avg_budget'),
            avg_runtime=analytics_data.get('avg_runtime'),
            username=analytics_data.get('username', ''),
            scraped_at=analytics_data.get('scraped_at', ''),
            snapshot_version=analytics_data.get('snapshot_version', SNAPSHOT_SCHEMA_VERSION)
        )
        
        return snapshot
    
    def to_json(self, indent: int = 2) -> str:
        """Convert snapshot to JSON string."""
        return json.dumps(self.to_dict(), indent=indent, ensure_ascii=False)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'ApplicationSnapshot':
        """Create snapshot from JSON string."""
        data = json.loads(json_str)
        return cls.from_dict(data)
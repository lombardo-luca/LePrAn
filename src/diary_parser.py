"""
Diary CSV parsing layer for LePrAn.

Parses diary.csv exports from Letterboxd and provides normalized
data structures for analytics aggregation. Uses Watched Date as
the primary date field.

Strict rules:
- No UI changes
- No chart logic
- No integration, only data transformation layer
"""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional


@dataclass
class DiaryEntry:
    """A single parsed entry from the diary.csv file."""
    watched_date: str          # Raw Watched Date (YYYY-MM-DD)
    watched_year: int          # Extracted year
    watched_month: int         # Extracted month (1-12)
    watched_day: int           # Extracted day (1-31)
    watched_weekday: str       # e.g., "Monday"
    diary_date: str            # Original Date column (metadata)
    film_name: str
    film_year: int
    letterboxd_uri: str
    rating: str
    is_rewatch: bool
    tags: str


def parse_diary_csv(filepath: Optional[str] = None) -> list[DiaryEntry]:
    """Parse diary.csv and return a list of DiaryEntry objects.

    Args:
        filepath: Path to diary.csv. Defaults to data/diary.csv
                  relative to the project root.

    Returns:
        List of DiaryEntry objects with normalized date fields.
    """
    if filepath is None:
        filepath = Path(__file__).parent.parent / "data" / "diary.csv"
    else:
        filepath = Path(filepath)

    entries: list[DiaryEntry] = []

    with open(filepath, "r", encoding="utf-8") as f:
        lines = f.readlines()

    if not lines:
        return entries

    # Parse header to find column indices
    header_line = lines[0].strip()
    headers = _parse_csv_line(header_line)

    date_idx = headers.index("Date") if "Date" in headers else -1
    name_idx = headers.index("Name") if "Name" in headers else -1
    year_idx = headers.index("Year") if "Year" in headers else -1
    uri_idx = headers.index("Letterboxd URI") if "Letterboxd URI" in headers else -1
    rating_idx = headers.index("Rating") if "Rating" in headers else -1
    rewatch_idx = headers.index("Rewatch") if "Rewatch" in headers else -1
    tags_idx = headers.index("Tags") if "Tags" in headers else -1
    watched_date_idx = headers.index("Watched Date") if "Watched Date" in headers else -1

    for line in lines[1:]:
        line = line.strip()
        if not line:
            continue

        fields = _parse_csv_line(line)

        if watched_date_idx < 0 or watched_date_idx >= len(fields):
            continue

        watched_date_str = fields[watched_date_idx].strip() if watched_date_idx < len(fields) else ""
        if not watched_date_str:
            continue

        try:
            watched_dt = datetime.strptime(watched_date_str, "%Y-%m-%d")
        except ValueError:
            continue

        diary_date = fields[date_idx].strip() if date_idx >= 0 and date_idx < len(fields) else ""
        film_name = fields[name_idx].strip() if name_idx >= 0 and name_idx < len(fields) else ""
        film_year_str = fields[year_idx].strip() if year_idx >= 0 and year_idx < len(fields) else ""
        letterboxd_uri = fields[uri_idx].strip() if uri_idx >= 0 and uri_idx < len(fields) else ""
        rating = fields[rating_idx].strip() if rating_idx >= 0 and rating_idx < len(fields) else ""
        rewatch = fields[rewatch_idx].strip() if rewatch_idx >= 0 and rewatch_idx < len(fields) else ""
        tags = fields[tags_idx].strip() if tags_idx >= 0 and tags_idx < len(fields) else ""

        try:
            film_year = int(film_year_str) if film_year_str else 0
        except ValueError:
            film_year = 0

        entry = DiaryEntry(
            watched_date=watched_date_str,
            watched_year=watched_dt.year,
            watched_month=watched_dt.month,
            watched_day=watched_dt.day,
            watched_weekday=watched_dt.strftime("%A"),
            diary_date=diary_date,
            film_name=film_name,
            film_year=film_year,
            letterboxd_uri=letterboxd_uri,
            rating=rating,
            is_rewatch=rewatch.lower() == "yes",
            tags=tags,
        )
        entries.append(entry)

    return entries


def aggregate_by_weekday(entries: list[DiaryEntry]) -> dict[str, int]:
    """Aggregate watch count by weekday name.

    Args:
        entries: List of DiaryEntry objects.

    Returns:
        Dictionary mapping weekday name to count.
    """
    result: dict[str, int] = {}
    for entry in entries:
        weekday = entry.watched_weekday
        result[weekday] = result.get(weekday, 0) + 1
    return result


def aggregate_by_month(entries: list[DiaryEntry]) -> dict[str, int]:
    """Aggregate watch count by year-month.

    Args:
        entries: List of DiaryEntry objects.

    Returns:
        Dictionary mapping "YYYY-MM" string to count.
    """
    result: dict[str, int] = {}
    for entry in entries:
        month_key = f"{entry.watched_year:04d}-{entry.watched_month:02d}"
        result[month_key] = result.get(month_key, 0) + 1
    return result


def aggregate_by_year(entries: list[DiaryEntry]) -> dict[str, int]:
    """Aggregate watch count by year.

    Args:
        entries: List of DiaryEntry objects.

    Returns:
        Dictionary mapping year string to count.
    """
    result: dict[str, int] = {}
    for entry in entries:
        year_key = str(entry.watched_year)
        result[year_key] = result.get(year_key, 0) + 1
    return result


def aggregate_by_film_rewatch(entries: list[DiaryEntry]) -> dict[str, int]:
    """Aggregate watch count by film name (including rewatch counts).

    Args:
        entries: List of DiaryEntry objects.

    Returns:
        Dictionary mapping film name to total watch count.
    """
    result: dict[str, int] = {}
    for entry in entries:
        name = entry.film_name
        result[name] = result.get(name, 0) + 1
    return result


# ---- Internal helpers ----

def _parse_csv_line(line: str) -> list[str]:
    """Parse a single CSV line handling quoted fields.

    Handles fields enclosed in double quotes that may contain commas.
    Example: "Monsters, Inc." is treated as a single field.

    Args:
        line: A raw CSV line string.

    Returns:
        List of field values.
    """
    fields: list[str] = []
    current: list[str] = []
    in_quotes = False
    i = 0

    while i < len(line):
        char = line[i]

        if in_quotes:
            if char == '"':
                # Check for escaped quote ""
                if i + 1 < len(line) and line[i + 1] == '"':
                    current.append('"')
                    i += 2
                    continue
                else:
                    in_quotes = False
            else:
                current.append(char)
        else:
            if char == '"':
                in_quotes = True
            elif char == ',':
                fields.append(''.join(current))
                current = []
            else:
                current.append(char)
        i += 1

    # Append the last field
    fields.append(''.join(current))

    return fields
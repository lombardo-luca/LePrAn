"""
Data management functionality.
Handles saving and loading CSV files, GUI display formatting, and data population.
"""
import csv
import logging
from pathlib import Path


# Configure logging
logger = logging.getLogger(__name__)


# Custom exceptions for better error handling
class DataManagerError(Exception):
    """Base exception for data manager operations."""
    pass


class CSVError(DataManagerError):
    """Exception raised for CSV file operations."""
    pass


class DataValidationError(DataManagerError):
    """Exception raised for data validation issues."""
    pass


class GUIGenerationError(DataManagerError):
    """Exception raised for GUI string generation issues."""
    pass


class StatisticsCSVHandler:
    """Handles pure CSV I/O operations for statistics data."""
    
    def __init__(self, stats_data):
        """Initialize with statistics data reference."""
        self.stats_data = stats_data
    
    def save_to_csv(self, username, scraped_at, films_num, total_hours, total_days, csv_path):
        """Save statistics to CSV file."""
        try:
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['section', 'name', 'count'])
                
                # Write metadata
                writer.writerow(['META', 'USER', username])
                writer.writerow(['META', 'SCRAPED_AT', scraped_at])
                writer.writerow(['META', 'FILMS', films_num])
                writer.writerow(['META', 'HOURS', f"{total_hours:.6f}"])
                writer.writerow(['META', 'DAYS', f"{total_days:.6f}"])

                # Write all statistics dictionaries
                for k, v in self.stats_data.lang_dict.items():
                    writer.writerow(['LANGUAGE', k, v])
                for k, v in self.stats_data.country_dict.items():
                    writer.writerow(['COUNTRY', k, v])
                for k, v in self.stats_data.genre_dict.items():
                    writer.writerow(['GENRE', k, v])
                for k, v in self.stats_data.director_dict.items():
                    writer.writerow(['DIRECTOR', k, v])
                for k, v in self.stats_data.actor_dict.items():
                    writer.writerow(['ACTOR', k, v])
                for k, v in self.stats_data.decade_dict.items():
                    writer.writerow(['DECADE', k, v])
                    
            logger.info(f"Successfully saved statistics to {csv_path}")
            return True
        except IOError as e:
            error_msg = f"Failed to write CSV file {csv_path}: {e}"
            logger.error(error_msg)
            raise CSVError(error_msg) from e
        except Exception as e:
            error_msg = f"Unexpected error saving CSV {csv_path}: {e}"
            logger.error(error_msg)
            raise CSVError(error_msg) from e
    
    def load_from_csv(self, csv_path):
        """Load statistics from CSV file and return metadata."""
        # Reset all data
        self.stats_data.reset()
        
        films_num = 0
        total_hours = 0.0
        total_days = 0.0
        loaded_username = ''
        loaded_scraped_at = ''

        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                for row in reader:
                    if len(row) < 3:
                        continue
                    section, name, count = row[0], row[1], row[2]
                    
                    if section == 'META':
                        if name == 'FILMS':
                            try:
                                films_num = int(count)
                            except Exception:
                                films_num = 0
                        elif name == 'USER':
                            loaded_username = count
                        elif name == 'SCRAPED_AT':
                            loaded_scraped_at = count
                        elif name == 'HOURS':
                            try:
                                total_hours = float(count)
                            except Exception:
                                total_hours = 0.0
                        elif name == 'DAYS':
                            try:
                                total_days = float(count)
                            except Exception:
                                total_days = 0.0
                    elif section == 'LANGUAGE':
                        self.stats_data.lang_dict[name] = int(count)
                    elif section == 'COUNTRY':
                        self.stats_data.country_dict[name] = int(count)
                    elif section == 'GENRE':
                        self.stats_data.genre_dict[name] = int(count)
                    elif section == 'DIRECTOR':
                        self.stats_data.director_dict[name] = int(count)
                    elif section == 'ACTOR':
                        self.stats_data.actor_dict[name] = int(count)
                    elif section == 'DECADE':
                        try:
                            self.stats_data.decade_dict[name] += int(count)
                        except (ValueError, KeyError) as e:
                            logger.warning(f"Failed to process decade data '{name}': {count} - {e}")
        except FileNotFoundError as e:
            error_msg = f"CSV file not found: {csv_path}"
            logger.error(error_msg)
            raise CSVError(error_msg) from e
        except IOError as e:
            error_msg = f"Failed to read CSV file {csv_path}: {e}"
            logger.error(error_msg)
            raise CSVError(error_msg) from e
        except Exception as e:
            error_msg = f"Unexpected error loading CSV {csv_path}: {e}"
            logger.error(error_msg)
            raise CSVError(error_msg) from e

        # Validate loaded data
        if films_num < 0:
            logger.warning(f"Invalid films count: {films_num}, setting to 0")
            films_num = 0
            
        if total_hours < 0:
            logger.warning(f"Invalid total hours: {total_hours}, setting to 0")
            total_hours = 0.0

        # Set meta data
        self.stats_data.set_meta_data(films_num, total_hours, total_days, loaded_scraped_at)
        
        logger.info(f"Successfully loaded statistics from {csv_path} - {films_num} films, {total_hours:.2f} hours")
        return {
            'films_num': films_num,
            'total_hours': total_hours,
            'total_days': total_days,
            'username': loaded_username,
            'scraped_at': loaded_scraped_at,
        }


class DataPopulator:
    """Handles populating GUI models with loaded data."""
    
    def __init__(self, gui_models, config):
        """Initialize with GUI models and configuration."""
        self.gui_models = gui_models
        self.config = config
    
    def populate_all_models(self, stats_data, films_num):
        """Populate all GUI models with statistics data."""
        try:
            if films_num < 0:
                raise DataValidationError(f"Films count cannot be negative: {films_num}")
            
            if not stats_data:
                raise DataValidationError("Statistics data cannot be None")
                
            self.gui_models.populate_model('countries', stats_data.country_dict, films_num, self.config.list_delim)
            self.gui_models.populate_model('languages', stats_data.lang_dict, films_num, self.config.list_delim)
            self.gui_models.populate_model('genres', stats_data.genre_dict, films_num, self.config.list_delim)
            self.gui_models.populate_model('directors', stats_data.director_dict, films_num, self.config.list_delim)
            self.gui_models.populate_model('actors', stats_data.actor_dict, films_num, self.config.list_delim)
            
            logger.debug(f"Successfully populated all GUI models for {films_num} films")
        except DataValidationError:
            raise  # Re-raise validation errors
        except Exception as e:
            error_msg = f"Failed to populate GUI models: {e}"
            logger.error(error_msg)
            raise DataManagerError(error_msg) from e


class GUIStringGenerator:
    """Handles generating display strings for GUI components."""
    
    def __init__(self, stats_data, config):
        """Initialize with statistics data and configuration."""
        self.stats_data = stats_data
        self.config = config
    
    def generate_all_strings(self, films_num):
        """Generate all GUI display strings from current statistics."""
        try:
            if films_num < 0:
                raise DataValidationError(f"Invalid films count: {films_num}")
                
            with self.stats_data.lock:
                self._generate_summary_strings(films_num)
                self._generate_language_strings(films_num)
                self._generate_country_strings(films_num)
                self._print_decade_stats(films_num)
                
            logger.debug(f"Generated GUI strings for {films_num} films")
        except Exception as e:
            error_msg = f"Failed to generate GUI strings: {e}"
            logger.error(error_msg)
            raise GUIGenerationError(error_msg) from e
    
    def _generate_summary_strings(self, films_num):
        """Generate summary strings (watched films and total time)."""
        self.stats_data.gui_watched1 = "Films watched: " + str(films_num)
        self.stats_data.gui_watched2 = "Total running time: " + "%.2f" % self.stats_data.total_hours + " hours (%.2f" % self.stats_data.total_days + " days)"
    
    def _generate_language_strings(self, films_num):
        """Generate language statistics strings."""
        sorted_lang = dict(sorted(self.stats_data.lang_dict.items(), key=lambda x: x[1], reverse=True))
        self.stats_data.gui_lang = "Language\tFilms\tPercentage\n\n"
        self.stats_data.gui_lang_list = [self.stats_data.gui_lang]
        
        cnt_lang = 0
        for k, v in sorted_lang.items():
            cnt_lang += 1
            if self.config.list_delim != -1 and cnt_lang > self.config.list_delim:
                break
            percent = (format(v / films_num * 100, ".2f") + "%") if films_num else "0.00%"
            self.stats_data.gui_lang += k + "\t" + str(v) + "\t" + percent + "\n"
            self.stats_data.gui_lang_list.append(k + "\t" + str(v) + "\t" + percent + "\n")
    
    def _generate_country_strings(self, films_num):
        """Generate country statistics strings."""
        sorted_country = dict(sorted(self.stats_data.country_dict.items(), key=lambda x: x[1], reverse=True))
        self.stats_data.gui_countries = "Country\tFilms\tPercentage\n\n"
        
        cnt_country = 0
        for k, v in sorted_country.items():
            cnt_country += 1
            if self.config.list_delim != -1 and cnt_country > self.config.list_delim:
                break
            percent = (format(v / films_num * 100, ".2f") + "%") if films_num else "0.00%"
            self.stats_data.gui_countries += k + "\t" + str(v) + "\t" + percent + "\n"
    
    def _print_decade_stats(self, films_num):
        """Print decade statistics to console."""
        try:
            sorted_decades = dict(sorted(self.stats_data.decade_dict.items(), key=lambda x: x[1], reverse=True))
            if sorted_decades:
                print("\nDecade            Films        Percentage")
                for k, v in sorted_decades.items():
                    percent = (format(v / films_num * 100, ".2f") + "%") if films_num else "0.00%"
                    print(f"{k:<20}{v:>10}{percent:>15}")
        except Exception as e:
            logger.warning(f"Failed to generate decade statistics: {e}")


class DataManager:
    """Coordinates data operations using specialized handler classes."""
    
    def __init__(self, app_context):
        """Initialize DataManager with dependency injection."""
        self.app_context = app_context
        self.stats_data = app_context.stats_data
        self.gui_models = app_context.gui_models
        self.config = app_context.config
        
        # Initialize specialized handlers
        self.csv_handler = StatisticsCSVHandler(self.stats_data)
        self.data_populator = DataPopulator(self.gui_models, self.config)
        self.gui_generator = GUIStringGenerator(self.stats_data, self.config)
    
    def save_stats_to_csv(self, username, scraped_at, films_num, total_hours, total_days, csv_path):
        """Save all extracted statistics to a CSV file."""
        try:
            # Validate inputs
            if not username:
                raise DataValidationError("Username cannot be empty")
            if films_num < 0:
                raise DataValidationError(f"Films count cannot be negative: {films_num}")
            if total_hours < 0:
                raise DataValidationError(f"Total hours cannot be negative: {total_hours}")
            if not csv_path:
                raise DataValidationError("CSV path cannot be empty")
                
            return self.csv_handler.save_to_csv(username, scraped_at, films_num, total_hours, total_days, csv_path)
        except (CSVError, DataValidationError):
            raise  # Re-raise our custom exceptions
        except Exception as e:
            error_msg = f"Unexpected error in save_stats_to_csv: {e}"
            logger.error(error_msg)
            raise DataManagerError(error_msg) from e
    
    def load_stats_from_csv(self, csv_path):
        """Load statistics from a CSV file into data structures."""
        try:
            if not csv_path:
                raise DataValidationError("CSV path cannot be empty")
            
            # Validate file exists
            csv_file = Path(csv_path)
            if not csv_file.exists():
                raise CSVError(f"CSV file does not exist: {csv_path}")
            
            # Reset GUI models
            self.gui_models.clear_all()
            
            # Load data using CSV handler
            meta = self.csv_handler.load_from_csv(csv_path)
            
            if meta:
                # Populate GUI models with loaded data
                self.data_populator.populate_all_models(self.stats_data, meta['films_num'])
                logger.info(f"Successfully loaded and populated data from {csv_path}")
            
            return meta
        except (CSVError, DataValidationError):
            raise  # Re-raise our custom exceptions
        except Exception as e:
            error_msg = f"Unexpected error in load_stats_from_csv: {e}"
            logger.error(error_msg)
            raise DataManagerError(error_msg) from e
    
    def generate_gui_strings(self, films_num):
        """Generate GUI display strings from current statistics."""
        try:
            self.gui_generator.generate_all_strings(films_num)
        except (GUIGenerationError, DataValidationError):
            raise  # Re-raise our custom exceptions
        except Exception as e:
            error_msg = f"Unexpected error in generate_gui_strings: {e}"
            logger.error(error_msg)
            raise DataManagerError(error_msg) from e

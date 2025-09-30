"""
Data management functionality.
Handles saving and loading CSV files, GUI display formatting, and data population.
"""
import csv
import time


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
                    
            print(f"Saved statistics to {csv_path}")
            return True
        except Exception as e:
            print(f"Error saving CSV: {e}")
            return False
    
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
                header = next(reader, None)
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
                        except Exception:
                            pass
        except FileNotFoundError:
            print(f"CSV not found: {csv_path}")
            return None
        except Exception as e:
            print(f"Error loading CSV: {e}")
            return None

        # Set meta data
        self.stats_data.set_meta_data(films_num, total_hours, total_days, loaded_scraped_at)
        
        print(f"Loaded statistics from {csv_path}")
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
        self.gui_models.populate_model('countries', stats_data.country_dict, films_num, self.config.list_delim)
        self.gui_models.populate_model('languages', stats_data.lang_dict, films_num, self.config.list_delim)
        self.gui_models.populate_model('genres', stats_data.genre_dict, films_num, self.config.list_delim)
        self.gui_models.populate_model('directors', stats_data.director_dict, films_num, self.config.list_delim)
        self.gui_models.populate_model('actors', stats_data.actor_dict, films_num, self.config.list_delim)


class GUIStringGenerator:
    """Handles generating display strings for GUI components."""
    
    def __init__(self, stats_data, config):
        """Initialize with statistics data and configuration."""
        self.stats_data = stats_data
        self.config = config
    
    def generate_all_strings(self, films_num):
        """Generate all GUI display strings from current statistics."""
        with self.stats_data.lock:
            self._generate_summary_strings(films_num)
            self._generate_language_strings(films_num)
            self._generate_country_strings(films_num)
            self._print_decade_stats(films_num)
    
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
        sorted_decades = dict(sorted(self.stats_data.decade_dict.items(), key=lambda x: x[1], reverse=True))
        if sorted_decades:
            print("\nDecade            Films        Percentage")
            for k, v in sorted_decades.items():
                percent = (format(v / films_num * 100, ".2f") + "%") if films_num else "0.00%"
                print(f"{k:<20}{v:>10}{percent:>15}")


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
        return self.csv_handler.save_to_csv(username, scraped_at, films_num, total_hours, total_days, csv_path)
    
    def load_stats_from_csv(self, csv_path):
        """Load statistics from a CSV file into data structures."""
        # Reset GUI models
        self.gui_models.clear_all()
        
        # Load data using CSV handler
        meta = self.csv_handler.load_from_csv(csv_path)
        
        if meta:
            # Populate GUI models with loaded data
            self.data_populator.populate_all_models(self.stats_data, meta['films_num'])
        
        return meta
    
    def generate_gui_strings(self, films_num):
        """Generate GUI display strings from current statistics."""
        self.gui_generator.generate_all_strings(films_num)

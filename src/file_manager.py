"""
File management functionality.
Handles saving and loading CSV files with statistics data.
"""
import csv
import time


class FileManager:
    """Handles CSV file operations for statistics data."""
    
    @staticmethod
    def save_stats_to_csv(username, scraped_at, films_num, total_hours, total_days, csv_path, app_context):
        """Save all extracted statistics to a CSV file."""
        try:
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['section', 'name', 'count'])
                writer.writerow(['META', 'USER', username])
                writer.writerow(['META', 'SCRAPED_AT', scraped_at])
                writer.writerow(['META', 'FILMS', films_num])
                writer.writerow(['META', 'HOURS', f"{total_hours:.6f}"])
                writer.writerow(['META', 'DAYS', f"{total_days:.6f}"])

                # Save all dictionaries
                for k, v in app_context.stats_data.lang_dict.items():
                    writer.writerow(['LANGUAGE', k, v])
                for k, v in app_context.stats_data.country_dict.items():
                    writer.writerow(['COUNTRY', k, v])
                for k, v in app_context.stats_data.genre_dict.items():
                    writer.writerow(['GENRE', k, v])
                for k, v in app_context.stats_data.director_dict.items():
                    writer.writerow(['DIRECTOR', k, v])
                for k, v in app_context.stats_data.actor_dict.items():
                    writer.writerow(['ACTOR', k, v])
                for k, v in app_context.stats_data.decade_dict.items():
                    writer.writerow(['DECADE', k, v])
            print(f"Saved statistics to {csv_path}")
            return True
        except Exception as e:
            print(f"Error saving CSV: {e}")
            return False

    @staticmethod
    def load_stats_from_csv(csv_path, app_context):
        """Load statistics from a CSV file into data structures."""
        # Reset all data
        app_context.stats_data.reset()
        app_context.gui_models.clear_all()

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
                        app_context.stats_data.lang_dict[name] = int(count)
                    elif section == 'COUNTRY':
                        app_context.stats_data.country_dict[name] = int(count)
                    elif section == 'GENRE':
                        app_context.stats_data.genre_dict[name] = int(count)
                    elif section == 'DIRECTOR':
                        app_context.stats_data.director_dict[name] = int(count)
                    elif section == 'ACTOR':
                        app_context.stats_data.actor_dict[name] = int(count)
                    elif section == 'DECADE':
                        try:
                            app_context.stats_data.decade_dict[name] += int(count)
                        except Exception:
                            pass
        except FileNotFoundError:
            print(f"CSV not found: {csv_path}")
            return None
        except Exception as e:
            print(f"Error loading CSV: {e}")
            return None

        # Set meta data
        app_context.stats_data.set_meta_data(films_num, total_hours, total_days, loaded_scraped_at)
        
        # Populate GUI models
        FileManager._populate_gui_models(films_num, app_context)

        print(f"Loaded statistics from {csv_path}")
        return {
            'films_num': films_num,
            'total_hours': total_hours,
            'total_days': total_days,
            'username': loaded_username,
            'scraped_at': loaded_scraped_at,
        }

    @staticmethod
    def _populate_gui_models(films_num, app_context):
        """Populate GUI models with loaded data."""
        # Populate each model
        app_context.gui_models.populate_model('countries', app_context.stats_data.country_dict, films_num, app_context.config.list_delim)
        app_context.gui_models.populate_model('languages', app_context.stats_data.lang_dict, films_num, app_context.config.list_delim)
        app_context.gui_models.populate_model('genres', app_context.stats_data.genre_dict, films_num, app_context.config.list_delim)
        app_context.gui_models.populate_model('directors', app_context.stats_data.director_dict, films_num, app_context.config.list_delim)
        app_context.gui_models.populate_model('actors', app_context.stats_data.actor_dict, films_num, app_context.config.list_delim)

    @staticmethod
    def generate_gui_strings(films_num, app_context):
        """Generate GUI display strings from current statistics."""
        with app_context.stats_data.lock:
            app_context.stats_data.gui_watched1 = "Films watched: " + str(films_num)
            app_context.stats_data.gui_watched2 = "Total running time: " + "%.2f" % app_context.stats_data.total_hours + " hours (%.2f" % app_context.stats_data.total_days + " days)"

            # Languages
            sorted_lang = dict(sorted(app_context.stats_data.lang_dict.items(), key=lambda x: x[1], reverse=True))
            app_context.stats_data.gui_lang = "Language\tFilms\tPercentage\n\n"
            app_context.stats_data.gui_lang_list = [app_context.stats_data.gui_lang]
            
            cnt_lang = 0
            for k, v in sorted_lang.items():
                cnt_lang += 1
                if app_context.config.list_delim != -1 and cnt_lang > app_context.config.list_delim:
                    break
                percent = (format(v / films_num * 100, ".2f") + "%") if films_num else "0.00%"
                app_context.stats_data.gui_lang += k + "\t" + str(v) + "\t" + percent + "\n"
                app_context.stats_data.gui_lang_list.append(k + "\t" + str(v) + "\t" + percent + "\n")

            # Countries
            sorted_country = dict(sorted(app_context.stats_data.country_dict.items(), key=lambda x: x[1], reverse=True))
            app_context.stats_data.gui_countries = "Country\tFilms\tPercentage\n\n"
            
            cnt_country = 0
            for k, v in sorted_country.items():
                cnt_country += 1
                if app_context.config.list_delim != -1 and cnt_country > app_context.config.list_delim:
                    break
                percent = (format(v / films_num * 100, ".2f") + "%") if films_num else "0.00%"
                app_context.stats_data.gui_countries += k + "\t" + str(v) + "\t" + percent + "\n"

            # Decades (for console output)
            sorted_decades = dict(sorted(app_context.stats_data.decade_dict.items(), key=lambda x: x[1], reverse=True))
            if sorted_decades:
                print("\nDecade            Films        Percentage")
                for k, v in sorted_decades.items():
                    percent = (format(v / films_num * 100, ".2f") + "%") if films_num else "0.00%"
                    print(f"{k:<20}{v:>10}{percent:>15}")

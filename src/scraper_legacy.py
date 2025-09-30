"""
Legacy web scraping functionality.
Handles scraping Letterboxd profiles and extracting film data.
"""
import requests
import re
import time
import json
import concurrent.futures
from bs4 import BeautifulSoup


class LegacyLetterboxdScraper:
    """Handles scraping Letterboxd profiles for film statistics."""
    
    def __init__(self, app_context):
        self.app_context = app_context
        self.session = None
        self.debug_times = {
            'time_1': -1,
            'tot_time_1': 0,
            'time_2': -1,
            'tot_time_2': 0,
            'time_3': -1,
            'tot_time_3': 0
        }
    
    def _create_session(self):
        """Create a requests session with proper headers."""
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
    
    def _scrape_film_page(self, url_film_page):
        """Scrape data from a single film page."""
        debug_start = time.time()
        source = self.session.get(url_film_page)
        end_time = time.time() - debug_start
        
        with self.app_context.stats_data.lock:
            self.debug_times['tot_time_1'] += end_time
            if end_time >= self.debug_times['time_1']:
                self.debug_times['time_1'] = end_time
        
        soup = BeautifulSoup(source.content, 'lxml')
        
        # Per-film unique buckets to avoid double counting
        film_languages = set()
        film_countries = set()
        film_genres = set()
        film_directors = set()
        film_actors = set()
        
        # Find release year
        year_found = self._extract_year(soup)
        decade = None
        if year_found is not None:
            decade = f"{year_found // 10 * 10}s"
        
        # Find runtime
        runtime = self._extract_runtime(soup)
        
        # Extract film data
        self._extract_languages(soup, film_languages)
        self._extract_countries(soup, film_countries)
        self._extract_genres(soup, film_genres)
        self._extract_directors(soup, film_directors)
        self._extract_actors(soup, film_actors)
        
        # Add to global statistics
        self.app_context.stats_data.add_film_data(
            film_languages, film_countries, film_genres,
            film_directors, film_actors, decade
        )
        
        return runtime
    
    def _extract_year(self, soup):
        """Extract release year from film page."""
        year_found = None
        
        # Try from visible releasedate link
        try:
            releasedate_link = soup.select_one('span.releasedate a')
            if releasedate_link and releasedate_link.text:
                yr_txt = re.search(r'(\d{4})', releasedate_link.text)
                if yr_txt:
                    year_found = int(yr_txt.group(1))
        except Exception:
            pass
        
        # Try from JSON-LD schema
        if year_found is None:
            try:
                for tag in soup.find_all('script', attrs={'type': 'application/ld+json'}):
                    data = json.loads(tag.string or '{}')
                    if isinstance(data, dict) and data.get('@type') in ('Movie', 'VideoObject'):
                        if isinstance(data.get('releasedEvent'), list) and data['releasedEvent']:
                            dt = data['releasedEvent'][0].get('startDate')
                            if dt:
                                yr_txt = re.search(r'(\d{4})', str(dt))
                                if yr_txt:
                                    year_found = int(yr_txt.group(1))
                                    break
                        dt = data.get('dateCreated') or data.get('datePublished')
                        if dt:
                            yr_txt = re.search(r'(\d{4})', str(dt))
                            if yr_txt:
                                year_found = int(yr_txt.group(1))
                                break
            except Exception:
                pass
        
        return year_found
    
    def _extract_runtime(self, soup):
        """Extract runtime from film page."""
        try:
            footer = soup.select_one('.text-link.text-footer')
            if footer:
                mt = re.search(r'(\d+)\s*min', footer.get_text(' '), re.I)
                if mt:
                    return int(mt.group(1))
        except Exception:
            pass
        return 0
    
    def _extract_languages(self, soup, film_languages):
        """Extract languages from film page."""
        try:
            lang_links = soup.select('#tab-details .text-sluglist a[href^="/films/language/"]')
            for a_tag in lang_links:
                lang = (a_tag.text or '').strip()
                if not lang:
                    continue
                if ',' in lang:
                    lang = lang.partition(',')[0]
                if lang == "No spoken language":
                    lang = "None"
                film_languages.add(lang)
        except Exception:
            pass
    
    def _extract_countries(self, soup, film_countries):
        """Extract countries from film page."""
        try:
            country_links = soup.select('#tab-details a[href^="/films/country/"]')
            for a_tag in country_links:
                country = (a_tag.text or '').strip()
                if not country:
                    continue
                if ',' in country:
                    country = country.partition(',')[0]
                film_countries.add(country)
        except Exception:
            pass
    
    def _extract_genres(self, soup, film_genres):
        """Extract genres from film page."""
        try:
            genre_links = soup.select('#tab-genres a[href^="/films/genre/"]')
            for a_tag in genre_links:
                genre = ((a_tag.text or '').strip()).capitalize()
                if not genre:
                    continue
                if ',' in genre:
                    genre = genre.partition(',')[0]
                film_genres.add(genre)
        except Exception:
            pass
    
    def _extract_directors(self, soup, film_directors):
        """Extract directors from film page."""
        try:
            # Production masthead credits
            credits = soup.select_one('section.production-masthead .details .credits')
            if credits:
                for a_tag in credits.select('a[href^="/director/"]'):
                    director = (a_tag.text or '').strip()
                    if director:
                        film_directors.add(director)
            
            # Crew tab explicit director section
            if not film_directors:
                for a_tag in soup.select('#tab-crew a[href^="/director/"]'):
                    director = (a_tag.text or '').strip()
                    if director:
                        film_directors.add(director)
        except Exception:
            pass
    
    def _extract_actors(self, soup, film_actors):
        """Extract actors from film page."""
        try:
            cast_items = soup.select('#tab-cast .cast-list a.text-slug, .cast-list.text-sluglist a.text-slug, .cast-list a[href^="/actor/"]')
            for a_tag in cast_items:
                actor = (a_tag.text or '').strip()
                if not actor:
                    continue
                low = actor.lower()
                if 'show all' in low or low.startswith('show '):
                    continue
                film_actors.add(actor)
        except Exception:
            pass
    
    def _get_films_from_page(self, url_table_page):
        """Get film URLs from a user's films page."""
        url_ltbxd = "https://letterboxd.com"
        source = self.session.get(url_table_page).text
        soup = BeautifulSoup(source, 'lxml')
        
        # Posters rendered as LazyPoster react components
        posters = soup.select('div.react-component[data-component-class="LazyPoster"]')
        count = 0
        for comp in posters:
            link = comp.get('data-item-link') or ''
            slug = comp.get('data-item-slug') or ''
            film_url = None
            if link:
                film_url = url_ltbxd + link
            elif slug:
                film_url = f"{url_ltbxd}/film/{slug}/"
            if film_url:
                self.app_context.stats_data.add_url(film_url)
                count += 1
                if count >= 72:
                    break
    
    def scrape_user_profile(self, username):
        """Scrape a complete user profile and return statistics."""
        self.app_context.stats_data.reset()
        self._create_session()
        
        print("Session:", self.session)
        print("Logging in " + username)
        
        # Verify that the user exists
        cnt = 1
        r = requests.get("https://letterboxd.com/" + username + "/films/page/" + str(cnt) + "/")
        soup = BeautifulSoup(r.text, 'lxml')
        str_match = str(soup)
        
        while "Sorry, we can't find the page" in str_match:
            print("There is no user named ", username, ".")
            username = input('Insert your Letterboxd username: ')
            cnt = 1
            r = requests.get("https://letterboxd.com/" + username + "/films/page/" + str(cnt) + "/")
            soup = BeautifulSoup(r.text, 'lxml')
            str_match = str(soup)
        
        print("Fetching data...")
        start_time = time.time()
        
        # Traverse pages by following pagination
        cnt = 1
        while True:
            st = "https://letterboxd.com/" + username + "/films/page/" + str(cnt) + "/"
            self._get_films_from_page(st)
            r = self.session.get(st)
            soup = BeautifulSoup(r.text, 'lxml')
            
            # Look for next page link
            next_link = (
                soup.select_one('div.pagination a.next') or
                soup.select_one('a.next[rel="next"]') or
                soup.select_one('div.paginate-nextprev a.next')
            )
            if next_link is None:
                break
            cnt += 1
        
        # Scrape all film pages
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.app_context.config.max_threads) as executor:
            runtime_list = list(executor.map(self._scrape_film_page, self.app_context.stats_data.url_list))
        
        films_num = len(self.app_context.stats_data.url_list)
        print("\nFilms watched: " + str(films_num))
        
        hrs = sum(runtime_list) / 60
        dys = hrs / 24
        
        print("Note: one film can have multiple languages and/or countries, so the sum of all percentages may be more than 100%.\n")
        
        # Set meta data
        try:
            scraped_when = time.strftime("%d/%m/%Y", time.localtime())
        except Exception:
            scraped_when = ""
        
        self.app_context.stats_data.set_meta_data(films_num, hrs, dys, scraped_when)
        
        print("\nScraping time: %.2f seconds." % (time.time() - start_time))
        
        return {
            'films_num': films_num,
            'total_hours': hrs,
            'total_days': dys,
            'username': username,
            'scraped_at': scraped_when
        }

import sys  # only needed for access to command line arguments
import requests
import re
import time
import os
import os.path
import threading 
import concurrent.futures
import csv
import cchardet as chardet
import json
from PyQt6 import QtWidgets
from PyQt6.QtWidgets import QHeaderView
from PyQt6.QtGui import QStandardItemModel, QStandardItem, QPixmap, QAction
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from MainWindow import Ui_MainWindow
from Dialog2 import Ui_Dialog
from Settings import Ui_Dialog as Ui_Dialog_Settings
from bs4 import BeautifulSoup
from itertools import repeat
from collections import defaultdict

MAX_THREADS = 20
LIST_DELIM = 200

DEBUG_MAX_INDEX = -1
DEBUG_TIME_1 = -1
TOT_TIME_1 = 0
DEBUG_TIME_2 = -1
TOT_TIME_2 = 0
DEBUG_TIME_3 = -1
TOT_TIME_3 = 0

lock = threading.Lock()
url_list = []
url_set = set()
langDict = {}
countryDict = {}
genreDict = {}
directorDict = {}
actorDict = {}
decadeDict = defaultdict(int)

gui_watched1 = ""
gui_watched2 = ""
gui_lang = ""
gui_lang_list = []
gui_countries = ""
gui_decades = ""

if getattr(sys, 'frozen', False):
    absolute_path = sys._MEIPASS
else:
    absolute_path = os.path.dirname(os.path.abspath(__file__))

# create model for countries
model1 = QStandardItemModel(0, 3)
model1.setHorizontalHeaderLabels(['Country', 'Films', 'Percentage'])

# create model for languages
model2 = QStandardItemModel(0, 3)
model2.setHorizontalHeaderLabels(['Language', 'Films', 'Percentage'])

# create model for genres
model3 = QStandardItemModel(0, 3)
model3.setHorizontalHeaderLabels(['Genre', 'Films', 'Percentage'])

# create model for directors
model4 = QStandardItemModel(0, 3)
model4.setHorizontalHeaderLabels(['Director', 'Films', 'Percentage'])

# create model for actors
model5 = QStandardItemModel(0, 3)
model5.setHorizontalHeaderLabels(['Actor', 'Films', 'Percentage'])

# need for py2exe --onefile
def resource_path(relative_path):
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

# get data from a film page
def scraper(url_film_page, requests_session):
    global DEBUG_TIME_1, TOT_TIME_1
    debug_start_1 = time.time()
    source = requests_session.get(url_film_page)
    end_time_1 = time.time() - debug_start_1
    soup = BeautifulSoup(source.content, 'lxml')
    
    with lock:
        TOT_TIME_1 += end_time_1
        if end_time_1 >= DEBUG_TIME_1:
            DEBUG_TIME_1 = end_time_1

    # per-film unique buckets to avoid double counting
    film_languages = set()
    film_countries = set()
    film_genres = set()
    film_directors = set()
    film_actors = set()

    # find release year
    year_found = None
    # try from visible releasedate link
    try:
        releasedate_link = soup.select_one('span.releasedate a')
        if releasedate_link and releasedate_link.text:
            yr_txt = re.search(r'(\d{4})', releasedate_link.text)
            if yr_txt:
                year_found = int(yr_txt.group(1))
    except Exception:
        pass
    # try from JSON-LD schema
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
    if year_found is not None:
        decade = f"{year_found // 10 * 10}s"
        with lock:
            decadeDict[decade] += 1

    # find runtime (from footer element)
    ret = 0
    try:
        footer = soup.select_one('.text-link.text-footer')
        if footer:
            mt = re.search(r'(\d+)\s*min', footer.get_text(' '), re.I)
            if mt:
                ret = int(mt.group(1))
    except Exception:
        pass

    # find languages (from tab structure)
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
        if lang_links:
            b = -1  # skip eventual fallback scan if we found via BS
    except Exception:
        pass
   
    # find countries (from details tab)
    found_country = False
    try:
        country_links = soup.select('#tab-details a[href^="/films/country/"]')
        for a_tag in country_links:
            country = (a_tag.text or '').strip()
            if not country:
                continue
            if ',' in country:
                country = country.partition(',')[0]
            film_countries.add(country)
            found_country = True
    except Exception:
        pass

    # find genres (from genres tab)
    found_genre = False
    try:
        genre_links = soup.select('#tab-genres a[href^="/films/genre/"]')
        for a_tag in genre_links:
            genre = ((a_tag.text or '').strip()).capitalize()
            if not genre:
                continue
            if ',' in genre:
                genre = genre.partition(',')[0]
            film_genres.add(genre)
            found_genre = True
    except Exception:
        pass

    # find directors (from productioninfo/crew)
    found_director = False
    try:
        # production masthead credits
        credits = soup.select_one('section.production-masthead .details .credits')
        if credits:
            for a_tag in credits.select('a[href^="/director/"]'):
                director = (a_tag.text or '').strip()
                if director:
                    film_directors.add(director)
                    found_director = True
        # crew tab explicit director section
        if not found_director:
            crew_dir = soup.select('#tab-crew + div, #tab-crew')  # ensure soup parses crew tab if present
            for a_tag in soup.select('#tab-crew a[href^="/director/"]'):
                director = (a_tag.text or '').strip()
                if director:
                    film_directors.add(director)
                    found_director = True
    except Exception:
        pass

    # find actors (from cast tab structure)
    found = 0
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
            found = 1
        # skip eventual fallback scan if we found via BS
        if found:
            b = -1
            c = -1
    except Exception:
        pass

    # add per-film unique buckets to global counters
    if film_languages:
        with lock:
            for lang in film_languages:
                langDict[lang] = langDict.get(lang, 0) + 1
    if film_countries:
        with lock:
            for country in film_countries:
                countryDict[country] = countryDict.get(country, 0) + 1
    if film_genres:
        with lock:
            for genre in film_genres:
                genreDict[genre] = genreDict.get(genre, 0) + 1
    if film_directors:
        with lock:
            for director in film_directors:
                directorDict[director] = directorDict.get(director, 0) + 1
    if film_actors:
        with lock:
            for actor in film_actors:
                actorDict[actor] = actorDict.get(actor, 0) + 1

    return ret

# get films' url
def getFilms(url_table_page, requests_session):
    global url_list
    global url_set
    url_ltbxd = "https://letterboxd.com"
    source = requests_session.get(url_table_page).text
    soup = BeautifulSoup(source, 'lxml')
    
    # posters rendered as LazyPoster react components
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
            if film_url not in url_set:
                url_set.add(film_url)
                url_list.append(film_url)
                count += 1
                if count >= 72:
                    break

def login(USER):
    global gui_watched1, gui_watched2, gui_lang, gui_lang_list, gui_countries
    cnt = 1
    r = requests.get("https://letterboxd.com/" + USER + "/films/page/" + str(cnt) + "/")
    soup = BeautifulSoup(r.text, 'lxml')
    str_match = str(soup)
    start_time = time.time()
    requests_session = requests.Session()
    
    # set headers to avoid being blocked and to get consistent markup
    requests_session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    })
    print("Session:", requests_session)
    print("Logging in " + USER)

    while "Sorry, we can’t find the page" in str_match:
        print("There is no user named ", USER, ".")
        USER = input('Insert your Letterboxd username: ')
        cnt = 1
        r = requests.get("https://letterboxd.com/" + USER + "/films/page/" + str(cnt) + "/")
        soup = BeautifulSoup(r.text, 'lxml')
        str_match = str(soup)

    print("Fetching data...")
    # traverse pages by following the pagination "next/older" link
    cnt = 1
    while True:
        st = "https://letterboxd.com/" + USER + "/films/page/" + str(cnt) + "/"
        getFilms(st, requests_session)
        r = requests_session.get(st)
        soup = BeautifulSoup(r.text, 'lxml')
        # look for next page link
        next_link = (
            soup.select_one('div.pagination a.next') or
            soup.select_one('a.next[rel="next"]') or
            soup.select_one('div.paginate-nextprev a.next')
        )
        if next_link is None:
            break
        cnt += 1
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        fat_list = list(executor.map(scraper, url_list, repeat(requests_session)))
            
    filmsNum = len(url_list)
    print("\nFilms watched: " + str(filmsNum))
    hrs = sum(fat_list) / 60
    dys = hrs / 24

    with lock:
        gui_watched1 = "Films watched: " + str(filmsNum)
        gui_watched2 = "Total running time: " + "%.2f" % hrs + " hours (%.2f" % dys + " days)"
    
    print("Note: one film can have multiple languages and/or countries, so the sum of all percentages may be more than 100%.\n")
    
    # process languages
    sortedLang = dict(sorted(langDict.items(), key=lambda x: x[1], reverse=True))
    spacing = max([len(i) for i in langDict.keys()] + [20]) + 1 if langDict else 21
    print(f"{'Language':<{spacing}}{'Films':>10}{'Percentage':>15}")
    gui_lang = "Language\tFilms\tPercentage\n\n"
    gui_lang_list.clear()
    gui_lang_list.append("Language\tFilms\tPercentage\n\n")
    cnt_lang = 0
    for k, v in sortedLang.items():
        cnt_lang += 1
        if LIST_DELIM != -1 and cnt_lang > LIST_DELIM:
            break
        percent = format(v / filmsNum * 100, ".2f") + "%"
        print(f"{k:<{spacing}}{v:>10}{percent:>15}")
        with lock:
            gui_lang += k + "\t" + str(v) + "\t" + percent + "\n"
            gui_lang_list.append(k + "\t" + str(v) + "\t" + percent + "\n")
            model2.appendRow([QStandardItem(k), QStandardItem(str(v)), QStandardItem(percent)])
    
    # process countries
    sortedCountry = dict(sorted(countryDict.items(), key=lambda x: x[1], reverse=True))
    spacing = max([len(i) for i in countryDict.keys()] + [20]) + 1 if countryDict else 21
    print(f"\n{'Country':<{spacing}}{'Films':>10}{'Percentage':>15}")
    with lock:
        gui_countries = "Country\tFilms\tPercentage\n\n"
    cnt_country = 0
    for k, v in sortedCountry.items():
        cnt_country += 1
        if LIST_DELIM != -1 and cnt_country > LIST_DELIM:
            break
        percent = format(v / filmsNum * 100, ".2f") + "%"
        print(f"{k:<{spacing}}{v:>10}{percent:>15}")
        with lock:
            gui_countries += k + "\t" + str(v) + "\t" + percent + "\n"
            model1.appendRow([QStandardItem(k), QStandardItem(str(v)), QStandardItem(percent)])
    
    # process genres
    sortedGenre = dict(sorted(genreDict.items(), key=lambda x: x[1], reverse=True))
    spacing = max([len(i) for i in genreDict.keys()] + [20]) + 1 if genreDict else 21
    print(f"\n{'Genre':<{spacing}}{'Films':>10}{'Percentage':>15}")
    cnt_genre = 0
    for k, v in sortedGenre.items():
        cnt_genre += 1
        if LIST_DELIM != -1 and cnt_genre > LIST_DELIM:
            break
        percent = format(v / filmsNum * 100, ".2f") + "%"
        print(f"{k:<{spacing}}{v:>10}{percent:>15}")
        model3.appendRow([QStandardItem(k), QStandardItem(str(v)), QStandardItem(percent)])
    
    # process directors
    sortedDirector = dict(sorted(directorDict.items(), key=lambda x: x[1], reverse=True))
    spacing = max([len(i) for i in directorDict.keys()] + [20]) + 1 if directorDict else 21
    print(f"\n{'Director':<{spacing}}{'Films':>10}{'Percentage':>15}")
    cnt_director = 0
    for k, v in sortedDirector.items():
        cnt_director += 1
        if LIST_DELIM != -1 and cnt_director > LIST_DELIM:
            break
        percent = format(v / filmsNum * 100, ".2f") + "%"
        print(f"{k:<{spacing}}{v:>10}{percent:>15}")
        model4.appendRow([QStandardItem(k), QStandardItem(str(v)), QStandardItem(percent)])
    
    # process actors
    sortedActor = dict(sorted(actorDict.items(), key=lambda x: x[1], reverse=True))
    spacing = max([len(i) for i in actorDict.keys()] + [20]) + 1 if actorDict else 21
    print(f"\n{'Actor':<{spacing}}{'Films':>10}{'Percentage':>15}")
    cnt_actor = 0
    for k, v in sortedActor.items():
        cnt_actor += 1
        if LIST_DELIM != -1 and cnt_actor > LIST_DELIM:
            break
        percent = format(v / filmsNum * 100, ".2f") + "%"
        print(f"{k:<{spacing}}{v:>10}{percent:>15}")
        model5.appendRow([QStandardItem(k), QStandardItem(str(v)), QStandardItem(percent)])
        
    # process decades (only GUI for now)
    sortedDecades = dict(sorted(decadeDict.items(), key=lambda x: x[1], reverse=True))
    spacing = max([len(i) for i in decadeDict.keys()] + [20]) + 1 if decadeDict else 21
    print(f"\n{'Decade':<{spacing}}{'Films':>10}{'Percentage':>15}")
    cnt_decade = 0
    for k, v in sortedDecades.items():
        cnt_decade += 1
        if LIST_DELIM != -1 and cnt_decade > LIST_DELIM:
            break
        percent = format(v / filmsNum * 100, ".2f") + "%"
        print(f"{k:<{spacing}}{v:>10}{percent:>15}")
    
    print("\nScraping time: %.2f seconds." % (time.time() - start_time))
    
    # with open("lboxd.csv", "w", encoding="UTF8") as f:
    #     writer = csv.writer(f)
    #     writer.writerow(sortedLang.keys())
    #     writer.writerow(sortedLang.values())

def refresher_thread(watched, lang, langBox, countries, countriesBox):
    langBox.repeat(1000, refresher)

def refresher():
    watched.value = gui_watched
    lang.value = gui_lang
    countries.value = gui_countries
    decades.value = gui_decades
    if lang.value != "":
        langBox.visible = True
    if countries.value != "":
        countriesBox.visible = True

class LoginThread(QThread):
    doneSignal = pyqtSignal()

    def __init__(self, login):
        super().__init__()
        self.login = login

    def run(self):
        login(self.login)
        self.doneSignal.emit()

class MainWindow(QtWidgets.QMainWindow, Ui_MainWindow):
    def __init__(self, *args, obj=None, **kwargs):
        super(MainWindow, self).__init__(*args, **kwargs)
        self.setupUi(self)

        global MAX_THREADS

        # create results window (dialog)
        self.dialog = QtWidgets.QDialog(self)
        self.ui = Ui_Dialog()
        self.ui.setupUi(self.dialog)

        self.change_settings_action = QAction("Change settings", self)
        self.change_settings_action.triggered.connect(self.open_settings_dialog)
        self.menuOptions.addAction(self.change_settings_action)

        # set pictures (logos)
        self.logo = QPixmap(resource_path('gfx/logo.png'))
        self.logoSmaller = QPixmap(resource_path('gfx/logoSmaller.png'))
        self.label.setPixmap(self.logo)
        self.ui.label_logo.setPixmap(self.logoSmaller)
        
        self.loginInput = None
        self.lineEdit.setFocusPolicy(Qt.FocusPolicy.ClickFocus)
        self.pushButton.clicked.connect(self.analyze)

        # read config file
        config_path = resource_path('cfg/config.txt')
        if os.path.exists(config_path):
            with open(config_path) as f:
                first_line = f.readline().strip('\n')
            print("Config file found.")
            splittedLine = first_line.split(':')
            if splittedLine[0] == 'workerThreadsNumber':
                MAX_THREADS = int(splittedLine[1])
                print("First line read.")
        else:
            with open(config_path, 'w') as f:
                f.write("workerThreadsNumber:20")
            print("Config file created.")
                
    def analyze(self):
        # reset globals for new search
        global url_list, url_set, langDict, countryDict, genreDict, directorDict, actorDict, decadeDict
        global gui_watched1, gui_watched2, gui_lang, gui_lang_list, gui_countries, gui_decades
        url_list = []
        url_set = set()
        langDict = {}
        countryDict = {}
        genreDict = {}
        directorDict = {}
        actorDict = {}
        decadeDict = defaultdict(int)
        gui_watched1 = ""
        gui_watched2 = ""
        gui_lang = ""
        gui_lang_list = []
        gui_countries = ""
        gui_decades = ""
        
        # clear all models
        model1.removeRows(0, model1.rowCount())
        model2.removeRows(0, model2.rowCount())
        model3.removeRows(0, model3.rowCount())
        model4.removeRows(0, model4.rowCount())
        model5.removeRows(0, model5.rowCount())

        self.pushButton.setEnabled(False)
        self.pushButton.setText("Analyzing...")
        
        self.loginInput = self.lineEdit.text()
        print("Name: " + self.loginInput)

        # run login function inside of a thread
        self.thread = LoginThread(self.loginInput)
        self.thread.doneSignal.connect(self.loginComplete)
        self.thread.start()

    def open_settings_dialog(self):
        global MAX_THREADS
        self.dialogSettings = QtWidgets.QDialog(self)
        self.settings = Ui_Dialog_Settings()
        self.settings.setupUi(self.dialogSettings)
        self.settings.spinBox.setValue(int(MAX_THREADS))
        print("Max threads1: " + str(MAX_THREADS))
        def save():
            global MAX_THREADS
            MAX_THREADS = self.settings.spinBox.value()
            with open(resource_path('cfg/config.txt'), 'w') as f:
                f.write("workerThreadsNumber:" + str(MAX_THREADS))
            print("Max threads2: " + str(MAX_THREADS))
        self.settings.save_button = QtWidgets.QDialogButtonBox.StandardButton.Save
        self.dialogSettings.accepted.connect(save)
        self.dialogSettings.show()

    def loginComplete(self):
        # re-enable the Analyze button for new searches
        self.pushButton.setText("Analyze")
        self.pushButton.setEnabled(True)
        
        self.ui.label_username.setText("User: " + self.loginInput)
        self.ui.label_results.setText(gui_watched1)
        self.ui.label_results2.setText(gui_watched2)

        # top left tableview (Countries)
        self.ui.tableView_1.setModel(model1)
        self.header1 = self.ui.tableView_1.horizontalHeader()       
        self.header1.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.header1.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        self.header1.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)

        # top center tableview (Languages)
        self.ui.tableView_2.setModel(model2)
        self.header2 = self.ui.tableView_2.horizontalHeader()       
        self.header2.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.header2.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        self.header2.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)

        # top right tableview (Genres)
        self.ui.tableView_3.setModel(model3)
        self.header3 = self.ui.tableView_3.horizontalHeader()       
        self.header3.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.header3.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        self.header3.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)

        # bottom left tableview (Directors)
        self.ui.tableView_botLeft.setModel(model4)
        self.header4 = self.ui.tableView_botLeft.horizontalHeader()       
        self.header4.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.header4.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        self.header4.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)

        # bottom center tableview (Actors)
        self.ui.tableView_botCenter.setModel(model5)
        self.header5 = self.ui.tableView_botCenter.horizontalHeader()       
        self.header5.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.header5.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        self.header5.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)

        self.dialog.show()

app = QtWidgets.QApplication(sys.argv)
window = MainWindow()
window.show()
app.exec()  # start the event loop
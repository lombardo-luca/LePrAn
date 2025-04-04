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
    str_match = str(soup)
    
    with lock:
        TOT_TIME_1 += end_time_1
        if end_time_1 >= DEBUG_TIME_1:
            DEBUG_TIME_1 = end_time_1

    # find release year
    release_year_match = re.search(r'data\.production\.releaseYear\s*=\s*(\d{4})', str_match)
    if release_year_match:
        try:
            year_found = int(release_year_match.group(1))
            decade = f"{year_found // 10 * 10}s"
            with lock:
                decadeDict[decade] += 1
        except ValueError:
            pass

    # find runtime
    a = str_match.find('text-link text-footer">') + len('text-link text-footer">')

    # find languages
    b = 0
    while b != -1:
        b = str_match.find('/films/language/', b)
        if b == -1:
            break
        index = max(0, b - 2000)
        check_spoken = str_match[index:b]
        if "Spoken Languages" in check_spoken:
            b = -1
            break
        b += len('/films/language/')
        b = str_match.find('>', b)
        if b == -1:
            break
        b += 1
        c = str_match.find('<', b)
        if c == -1:
            break
        lang = str_match[b:c]
        if ',' in lang:
            lang = lang.partition(',')[0]
        if lang == "No spoken language":
            lang = "None"
        with lock:
            langDict[lang] = langDict.get(lang, 0) + 1

    # find countries
    b = 0
    while b != -1:
        b = str_match.find('/films/country/', b)
        if b == -1:
            break
        b += len('/films/country/')
        b = str_match.find('>', b)
        if b == -1:
            break
        b += 1
        c = str_match.find('<', b)
        if c == -1:
            break
        country = str_match[b:c]
        if ',' in country:
            country = country.partition(',')[0]
        with lock:
            countryDict[country] = countryDict.get(country, 0) + 1

    # find genres
    b = 0
    while b != -1:
        b = str_match.find('/films/genre/', b)
        if b == -1:
            break
        b += len('/films/genre/')
        b = str_match.find('>', b)
        if b == -1:
            break
        b += 1
        c = str_match.find('<', b)
        if c == -1:
            break
        genre = str_match[b:c].capitalize()
        if ',' in genre:
            genre = genre.partition(',')[0]
        with lock:
            genreDict[genre] = genreDict.get(genre, 0) + 1

    # find directors
    d = str_match.find('more-directors', 0)
    if d != -1:
        b = d
        while b != -1:
            b = str_match.find("/director", b)
            if b == -1:
                break
            e = str_match.find("</span>", b)
            if e != -1 and e < str_match.find("</p>", b):
                break
            b += len('/director')
            b = str_match.find('/">', b)
            if b == -1:
                break
            b += len('/">')
            c = str_match.find('<', b)
            if c == -1:
                break
            director = str_match[b:c]
            with lock:
                directorDict[director] = directorDict.get(director, 0) + 1
    else:
        d = str_match.find("Directed by </span><span")
        b = d
        while b != -1:
            b = str_match.find("/director", b)
            if b == -1:
                break
            e = str_match.find("</section>", d, b)
            if e != -1:
                break
            b += len('/director')
            b = str_match.find('prettify">', b)
            if b == -1:
                break
            b += len('prettify">')
            c = str_match.find('<', b)
            if c == -1:
                break
            director = str_match[b:c]
            with lock:
                directorDict[director] = directorDict.get(director, 0) + 1

    # find actors
    b = 0
    found = 0
    while found == 0:
        b = str_match.find('cast-list text-sluglist">', b)
        if b == -1:
            break
        b += len('cast-list text-sluglist">')
        b = str_match.find('">', b)
        if b == -1:
            break
        b += len('">')
        c = str_match.find('</', b)
        if c == -1:
            break
        d = str_match.find("remove-ads-modal", b, c)
        if d != -1:
            b = c
            continue
        actor = str_match[b:c].strip()
        if '<' in actor or '>' in actor:
            b = c
            continue
        with lock:
            actorDict[actor] = actorDict.get(actor, 0) + 1
        found = 1

    while found == 1 and b != -1 and c != -1:
        b = str_match.find('/actor/', b)
        if b == -1:
            break
        b += len('/actor/')
        b = str_match.find('">', b)
        if b == -1:
            break
        b += len('">')
        c = str_match.find('</', b)
        if c == -1:
            break
        d = str_match.find("remove-ads-modal", b, c)
        if d != -1:
            b = c
            continue
        actor = str_match[b:c].strip()
        if '<' in actor or '>' in actor:
            b = c
            continue
        with lock:
            actorDict[actor] = actorDict.get(actor, 0) + 1

    try:
        ret = int(''.join(map(str, re.findall(r'\d*\.?\d+', str_match[a:a+20]))))
        return ret
    except:
        return 0

# get films' url
def getFilms(url_table_page):
    global url_list
    a = 0
    b = 0
    url_ltbxd = "https://letterboxd.com/film/"
    source = requests.get(url_table_page).text
    soup = BeautifulSoup(source, 'lxml')
    str_match = str(soup)
    while a <= len(str_match) and b < 72 and str_match.find('data-film-slug="', a) != -1:
        a = str_match.find('data-film-slug="', a) + len('data-film-slug="')
        helpstring = url_ltbxd
        while str_match[a] != '"':
            helpstring += str_match[a]
            a += 1
        url_list.append(helpstring)
        b += 1

def login(USER):
    global gui_watched1, gui_watched2, gui_lang, gui_lang_list, gui_countries
    cnt = 1
    r = requests.get("https://letterboxd.com/" + USER + "/films/page/" + str(cnt) + "/")
    soup = BeautifulSoup(r.text, 'lxml')
    str_match = str(soup)
    start_time = time.time()
    requests_session = requests.Session()
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
    while str_match.find('poster-container') != -1:
        cnt += 1
        r = requests.get("https://letterboxd.com/" + USER + "/films/page/" + str(cnt) + "/")
        soup = BeautifulSoup(r.text, 'lxml')
        str_match = str(soup)

    for i in range(1, cnt):
        st = "https://letterboxd.com/" + USER + "/films/page/" + str(i) + "/"
        getFilms(st)
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
    
    # Process languages
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
    
    # Process countries
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
        global url_list, langDict, countryDict, genreDict, directorDict, actorDict, decadeDict
        global gui_watched1, gui_watched2, gui_lang, gui_lang_list, gui_countries, gui_decades
        url_list = []
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
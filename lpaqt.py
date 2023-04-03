import sys # only needed for access to command line arguments
from PyQt6 import QtWidgets, uic, QtCore
from PyQt6.QtWidgets import QStyledItemDelegate, QHeaderView
from PyQt6.QtGui import QStandardItemModel, QStandardItem, QPixmap
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from MainWindow import Ui_MainWindow
from Dialog2 import Ui_Dialog
from bs4 import BeautifulSoup
import requests
import re
import time
import os
import threading 
import concurrent.futures
import csv

MAX_THREADS = 20
LIST_DELIM = 100

lock = threading.Lock();
url_list = [];
langDict = {};
countryDict = {};
genreDict = {};
directorDict = {};
actorDict = {};

gui_watched1 = "";
gui_watched2 = "";
gui_lang = "";
gui_lang_list = [];
gui_countries = "";

#create model for countries
model1 = QStandardItemModel(0, 3)
model1.setHorizontalHeaderLabels(['Country', 'Amount', 'Percentage'])

# create model for languages
model2 = QStandardItemModel(0, 3)
model2.setHorizontalHeaderLabels(['Language', 'Amount', 'Percentage'])

#create model for genres
model3 = QStandardItemModel(0, 3)
model3.setHorizontalHeaderLabels(['Genre', 'Amount', 'Percentage'])

#create model for directors
model4 = QStandardItemModel(0, 3)
model4.setHorizontalHeaderLabels(['Director', 'Amount', 'Percentage'])

#create model for actors
model5 = QStandardItemModel(0, 3)
model5.setHorizontalHeaderLabels(['Actor', 'Amount', 'Percentage'])

# need for py2exe --onefile
def resource_path(relative_path):
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")

    return os.path.join(base_path, relative_path)

# get data from a film page
def scraper(url_film_page):
        a = 0
        source  = requests.get(url_film_page).text
        soup = BeautifulSoup(source,'lxml')
        str_match = str(soup)

        # find runtime
        a = str_match.find('text-link text-footer">')
        a = a + len('text-link text-footer">')

        # find languages
        b = 0
        while b != -1:
                b = str_match.find('/films/language/', b)
                if b == -1:
                        break
                
                # check if this is an original or a spoken language
                index = max(0, b-2000)
                check_spoken = str_match[index:b]
                if "Spoken Languages" in check_spoken:
                        b = -1
                        break
                
                b = b + len('/films/language/')
                b = str_match.find('>', b)
                if b == -1:
                        break
                b = b+1
                c = str_match.find('<', b)
                if c == -1:
                        break

                lang = str_match[b:c]
                if ',' in lang:
                        lang = lang.partition(',')[0]

                if lang == "No spoken language":
                        lang = "None"
        
                lock.acquire()
                if lang in langDict:
                        langDict[lang] = langDict[lang] + 1
                else:
                        langDict[lang] = 1
                #print("I found the language" + lang + "in the film" + url_film_page)
                lock.release()

        # find countries
        b = 0
        while b != -1:
                b = str_match.find('/films/country/', b)
                if b == -1:
                        break
                b = b + len('/films/country/')
                b = str_match.find('>', b)
                if b == -1:
                        break
                b = b+1
                c = str_match.find('<', b)
                if c == -1:
                        break

                country = str_match[b:c]
                if ',' in country:
                        country = country.partition(',')[0]

                lock.acquire()
                if country in countryDict:
                        countryDict[country] = countryDict[country] + 1
                else:
                        countryDict[country] = 1
                lock.release()

        # find genres
        b = 0
        while b != -1:
                b = str_match.find('/films/genre/', b)
                if b == -1:
                        break
                b = b + len('/films/genre/')
                b = str_match.find('>', b)
                if b == -1:
                        break
                b = b+1
                c = str_match.find('<', b)
                if c == -1:
                        break

                genre = str_match[b:c].capitalize()
                if ',' in genre:
                        genre = genre.partition(',')[0]

                lock.acquire()
                if genre in genreDict:
                        genreDict[genre] = genreDict[genre] + 1
                else:
                        genreDict[genre] = 1
                lock.release()

        # find directors
        c = 0
        d = str_match.find('Directors<', 0)
        
        # if there are more then 2 directors
        if d != -1:   
                b = d
                while b != -1:
                        b = str_match.find("/director", b)
                        if b == -1:
                                break

                        e = str_match.find("</section>", d, b)
                        if e != -1:
                                break
                        b = b + len('/director')
                        b = str_match.find('/">', b)
                        if b == -1:
                                break
                        b = b + len('/">')
                        c = str_match.find('<', b)
                        if c == -1:
                                break
                        
                        director = str_match[b:c]

                        lock.acquire()
                        if director in directorDict:
                                directorDict[director] = directorDict[director] + 1
                        else:
                                directorDict[director] = 1
                        lock.release()

        # if there is only 1 or 2 directors
        else:
                d = str_match.find("Directed by <a href")
                b = d
                while b != -1:
                        b = str_match.find("/director", b)
                        if b == -1:
                                break

                        e = str_match.find("</section>", d, b)
                        if e != -1:
                                break
                        b = b + len('/director')
                        b = str_match.find('prettify">', b)
                        if b == -1:
                                break
                        b = b + len('prettify">')
                        c = str_match.find('<', b)
                        if c == -1:
                                break
                        
                        director = str_match[b:c]

                        lock.acquire()
                        if director in directorDict:
                                directorDict[director] = directorDict[director] + 1
                        else:
                                directorDict[director] = 1
                        lock.release()

        # find actors
        b = 0
        c = 0
        found = 0
        while found == 0:
                b = str_match.find('cast-list text-sluglist">', b)
                if b == -1:
                        break
                b = b + len('cast-list text-sluglist">')
                b = str_match.find('">', b)
                if b == -1:
                        break
                b = b + len('">')
                c = str_match.find('</', b)
                if c == -1:
                        break

                d = str_match.find("remove-ads-modal", b, c)
                if d != -1:
                        break

                actor = str_match[b:c]

                lock.acquire()
                if actor in actorDict:
                        actorDict[actor] = actorDict[actor] + 1
                else:
                        actorDict[actor] = 1
                lock.release()

                found = 1

        while found == 1 and b != -1 and c != -1:
                b = str_match.find('/actor/', b)
                if b == -1:
                        break
                b = b + len('/actor/')
                b = str_match.find('">', b)
                if b == -1:
                        break
                b = b + len('">')
                c = str_match.find('</', b)
                if c == -1:
                        break

                d = str_match.find("remove-ads-modal", b, c)
                if d != -1:
                        break

                actor = str_match[b:c]

                lock.acquire()
                if actor in actorDict:
                        actorDict[actor] = actorDict[actor] + 1
                else:
                        actorDict[actor] = 1
                lock.release()

        # return runtime
        try:
                ret = int(''.join(map(str,re.findall('\d*\.?\d+',str_match[a:a+20]))))
                return ret
        except:
                return 0

# get films' url
def getFilms(url_table_page):
        a = 0
        b = 0
        url_ltbxd = "https://letterboxd.com/film/"
        helpstring = ""
        source  = requests.get(url_table_page).text
        soup = BeautifulSoup(source,'lxml')
        str_match = str(soup)

        while(a <= len(str_match) and b < 72 and str_match.find('data-film-slug="/film/', a) != -1): #72 film in the table
                a = str_match.find('data-film-slug="/film/', a)
                a=a+len('data-film-slug="/film/')
                helpstring = url_ltbxd
                while str_match[a] != '/':
                    helpstring=helpstring+str_match[a]
                    a=a+1
                url_list.append(helpstring)
                helpstring=""
                b = b+1
        
        # debug
        #url_list.clear()
        #url_list.append("https://letterboxd.com/film/cloud-atlas/")
        #url_list.append("https://letterboxd.com/film/one-hundred-steps/")
        #url_list.append("https://letterboxd.com/film/the-banshees-of-inisherin/")

def login(USER):
        global gui_watched1
        global gui_watched2
        global gui_lang
        global gui_countries
        
        cnt = 1;
        r  = requests.get(("https://letterboxd.com/"+USER+ "/films/page/" + str(cnt) + "/"))
        soup = BeautifulSoup(r.text, 'lxml')
        str_match = str(soup)

        start_time = time.time()

        print("Logging in " + USER)

        while "Sorry, we can’t find the page" in str_match:
                print("There is no user named ", USER, ".")
                USER = input('Insert your Letterboxd username: ')
                cnt = 1;
                r  = requests.get(("https://letterboxd.com/"+USER+ "/films/page/" + str(cnt) + "/"))
                soup = BeautifulSoup(r.text, 'lxml')
                str_match = str(soup)

        print("Fetching data...")
        
        while str_match.find('poster-container') != -1 :
                cnt=cnt + 1
                r  = requests.get("https://letterboxd.com/"+USER+ "/films/page/" + str(cnt) + "/")
                soup = BeautifulSoup(r.text, 'lxml')
                str_match = str(soup)

        for i in range(1,cnt):
                st = str( ("https://letterboxd.com/"+USER+ "/films/page/" + str(i) + "/"))
                getFilms(st)
                i=i+1
                #start_time = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
                fat_list = executor.map(scraper, url_list)
                
        # print general stats
        filmsNum = len(url_list)
        print("\nFilms watched: " + str(filmsNum))
        hrs = sum(fat_list) / 60
        dys = hrs/24

        # update ui
        lock.acquire()
        gui_watched1 = "Films watched: " + str(filmsNum)
        gui_watched2 ="Total running time: " + "%.2f" %hrs + " hours (%.2f" %dys + " days)"

        lock.release()
 
        print("Note: one film can have multiple languages and/or countries, so the sum of all percentages may be more than 100%.\n")

        # print languages
        sortedLangValues = sorted(langDict.values(), reverse=True)
        sortedLang = dict(sorted(langDict.items(),key=lambda x:x[1],reverse = True))

        spacing = 20
        cnt = 0
        for i in langDict.keys():
            if LIST_DELIM != -1:
                cnt = cnt + 1
                if cnt > LIST_DELIM:
                    break
            if len(i) >= spacing:
                spacing = len(i)+1

        lock.acquire()  
        print(f"{'Language':<{spacing}}{'Films':>10}{'Percentage':>15}")
        gui_lang = "Language\tFilms\tPercentage\n\n"
        gui_lang_list.append("Language\tFilms\tPercentage\n\n")
        lock.release()
        cnt = 0
        for k, v in sortedLang.items():
                if LIST_DELIM != -1:
                        cnt = cnt+1
                        if cnt > LIST_DELIM:
                                break
                percent = format(v/filmsNum*100, ".2f") + "%"
                print(f"{k:<{spacing}}{v:>10}{percent:>15}")
                lock.acquire()
                gui_lang += k + "\t" + str(v) + "\t" + percent + "\n"
                gui_lang_list.append(k + "\t" + str(v) + "\t" + percent + "\n")

                # populate model inside of listView
                model2.appendRow([QStandardItem(k), QStandardItem(str(v)), QStandardItem(percent)])
                
                lock.release()
        
        # print countries
        print("")
        sortedCountryValues = sorted(countryDict.values(), reverse=True)
        sortedCountry = dict(sorted(countryDict.items(),key=lambda x:x[1],reverse = True))

        spacing = 20
        cnt = 0
        for i in countryDict.keys():
            if LIST_DELIM != -1:
                cnt = cnt + 1
                if cnt > LIST_DELIM:
                    break
            if len(i) >= spacing:
                spacing = len(i)+1
                
        print(f"{'Country':<{spacing}}{'Films':>10}{'Percentage':>15}")
        lock.acquire()
        gui_countries = "Country\tFilms\tPercentage\n\n"
        lock.release()
        
        cnt = 0
        for k, v in sortedCountry.items():
            if LIST_DELIM != -1:
                cnt = cnt+1
                if cnt > LIST_DELIM:
                    break
            percent = format(v/filmsNum*100, ".2f") + "%"
            print(f"{k:<{spacing}}{v:>10}{percent:>15}")
            lock.acquire()
            gui_countries += k + "\t" + str(v) + "\t" + percent + "\n"

            # populate model inside of listView
            model1.appendRow([QStandardItem(k), QStandardItem(str(v)), QStandardItem(percent)])
            
            lock.release()

        # print genres
        print("")
        sortedGenreValues = sorted(genreDict.values(), reverse=True)
        sortedGenre = dict(sorted(genreDict.items(),key=lambda x:x[1],reverse = True))

        spacing = 20
        cnt = 0
        for i in genreDict.keys():
            if LIST_DELIM != -1:
                cnt = cnt + 1
                if cnt > LIST_DELIM:
                    break
            if len(i) >= spacing:
                spacing = len(i)+1
                
        print(f"{'Genre':<{spacing}}{'Films':>10}{'Percentage':>15}")
        cnt = 0
        for k, v in sortedGenre.items():
                if LIST_DELIM != -1:
                        cnt = cnt+1
                        if cnt > LIST_DELIM:
                                break
                percent = format(v/filmsNum*100, ".2f") + "%"
                print(f"{k:<{spacing}}{v:>10}{percent:>15}")

                # populate model inside of listView
                model3.appendRow([QStandardItem(k), QStandardItem(str(v)), QStandardItem(percent)])

        # print directors
        print("")
        sortedDirectorValues = sorted(directorDict.values(), reverse=True)
        sortedDirector = dict(sorted(directorDict.items(),key=lambda x:x[1],reverse = True))

        spacing = 20
        cnt = 0
        for i in directorDict.keys():
            if LIST_DELIM != -1:
                cnt = cnt + 1
                if cnt > LIST_DELIM:
                    break
            if len(i) >= spacing:
                spacing = len(i)+1
                
        print(f"{'Director':<{spacing}}{'Films':>10}{'Percentage':>15}")
        cnt = 0
        for k, v in sortedDirector.items():
                if LIST_DELIM != -1:
                        cnt = cnt+1
                        if cnt > LIST_DELIM:
                                break
                percent = format(v/filmsNum*100, ".2f") + "%"
                print(f"{k:<{spacing}}{v:>10}{percent:>15}")

                # populate model inside of listView
                model4.appendRow([QStandardItem(k), QStandardItem(str(v)), QStandardItem(percent)])

        # print actors
        print("")
        sortedActorValues = sorted(actorDict.values(), reverse=True)
        sortedActor = dict(sorted(actorDict.items(),key=lambda x:x[1],reverse = True))

        spacing = 20
        cnt = 0
        for i in actorDict.keys():
            if LIST_DELIM != -1:
                cnt = cnt + 1
                if cnt > LIST_DELIM:
                    break
            if len(i) >= spacing:
                spacing = len(i)+1
                
        print(f"{'Actor':<{spacing}}{'Films':>10}{'Percentage':>15}")
        cnt = 0
        for k, v in sortedActor.items():
                if LIST_DELIM != -1:
                        cnt = cnt+1
                        if cnt > LIST_DELIM:
                                break
                percent = format(v/filmsNum*100, ".2f") + "%"
                print(f"{k:<{spacing}}{v:>10}{percent:>15}")

                # populate model inside of listView
                model5.appendRow([QStandardItem(k), QStandardItem(str(v)), QStandardItem(percent)])

        # print script stats
        print("\nScraping time: %.2f seconds." % (time.time() - start_time))

        with open("lboxd.csv", "w", encoding="UTF8") as f:
                writer = csv.writer(f)
                writer.writerow(sortedLang.keys())
                writer.writerow(sortedLang.values())

def refresher_thread(watched, lang, langBox, countries, countriesBox):
        langBox.repeat(1000, refresher)

def refresher():
        watched.value = gui_watched
        lang.value = gui_lang
        countries.value = gui_countries
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

                self.dialog = QtWidgets.QDialog(self)
                self.ui = Ui_Dialog()
                self.ui.setupUi(self.dialog)

                self.logo = QPixmap(resource_path('gfx/logo.png'))
                self.logoSmaller = QPixmap(resource_path('gfx/logoSmaller.png'))
                self.label.setPixmap(self.logo)
                self.ui.label_logo.setPixmap(self.logoSmaller)
                
                self.loginInput = None
                self.lineEdit.setFocusPolicy(Qt.FocusPolicy.ClickFocus)
                self.pushButton.clicked.connect(self.analyze)

        def analyze(self):
                # change button text
                self.pushButton.setEnabled(False)
                self.pushButton.setText("Analyzing...")
                
                self.loginInput = self.lineEdit.text()
                print("Name: " + self.loginInput)

                # run login function inside of a thread
                self.thread = LoginThread(self.loginInput)
                self.thread.doneSignal.connect(self.loginComplete)
                self.thread.start()

        def loginComplete(self):
                self.pushButton.setText("Done")

                self.ui.label_username.setText("User: " + self.loginInput)
                self.ui.label_results.setText(gui_watched1)
                self.ui.label_results2.setText(gui_watched2)

                # top left tableview
                self.ui.tableView_1.setModel(model1)
                self.header1 = self.ui.tableView_1.horizontalHeader()       
                self.header1.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
                self.header1.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
                self.header1.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)

                # top center tableview
                self.ui.tableView_2.setModel(model2)
                self.header2 = self.ui.tableView_2.horizontalHeader()       
                self.header2.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
                self.header2.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
                self.header2.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)

                # top right tableview
                self.ui.tableView_3.setModel(model3)
                self.header3 = self.ui.tableView_3.horizontalHeader()       
                self.header3.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
                self.header3.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
                self.header3.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)

                # bottom left tableview
                self.ui.tableView_botLeft.setModel(model4)
                self.header4 = self.ui.tableView_botLeft.horizontalHeader()       
                self.header4.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
                self.header4.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
                self.header4.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)

                # bottom center tableview
                self.ui.tableView_botCenter.setModel(model5)
                self.header5 = self.ui.tableView_botCenter.horizontalHeader()       
                self.header5.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
                self.header5.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
                self.header5.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)

                self.dialog.show()

app = QtWidgets.QApplication(sys.argv)

window = MainWindow()
window.show()
app.exec()      # start the event loop

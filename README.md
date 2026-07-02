# LePrAn: Letterboxd Profile Analyzer
<p align="center"><img src="https://i.imgur.com/1uoOUjs.png"></p>
<p align="center"><img alt="GitHub top language" src="https://img.shields.io/github/languages/top/lombardo-luca/LePrAn"> <img alt="GitHub code size in bytes" src="https://img.shields.io/github/languages/code-size/lombardo-luca/LePrAn"> <img alt="GitHub last commit (branch)" src="https://img.shields.io/github/last-commit/lombardo-luca/LePrAn/main"></p>

LePrAn takes your Letterboxd export data and turns it into actual statistics. The free version of Letterboxd itself doesn't give you much in the way of analytics, so this tool aims to fill that gap by pulling data from the TMDB API.

## Features

**Film stats**: Breakdowns by country, language, genre, director, actor, and decade. Per-film budget and box office rankings, plus budget range groupings. Total watch time in hours and days.

**Diary analytics**: See which days of the week, months, and years you watched the most films.

**Watchlist**: Optional import of your Letterboxd watchlist. Toggle between watched and watchlist views with separate stats.

**Snapshots**: Save your entire analysis as a JSON file and reload it later without re-scraping. 

**Theme toggle**: Dark (default) and light themes. Switch with the button in the top-right corner. Your choice is remembered.

## Screenshots (work in progress)
<img src="https://i.imgur.com/dtnQhni.png">
<img src="https://i.imgur.com/kK3L4xJ.png">
<img src="https://i.imgur.com/3maIkCJ.png">

## Usage

1. Run `python lepran.py`
2. If it's your first time running this specific version of LePrAn, enter your TMDB API key. It's free and you can get one [here](https://www.themoviedb.org/settings/api). The key is saved to `cfg/.env`.
3. Click "Analyze Letterboxd Folder" and pick your Letterboxd export directory (the extracted .zip file). It needs `watched.csv` and `diary.csv` at minimum, while `watchlist.csv` is optional: by default, it gets imported too, so uncheck that option if you don't want it.
4. Wait some time (usually a few minutes) for the analysis to complete.
5. Save a snapshot (full JSON state) so you can load it later without re-analyzing the data.

## Building

```bash
build.bat
```

Runs PyInstaller and puts a single executable in `dist/`.
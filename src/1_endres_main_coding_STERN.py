"""
Code zum Download aller Stern-Artikel und Ausgaben als Rohdaten (im HTML-Format) [### 1 und ### 2]. 
                                                                --> HTML-Seite des Artikels und HTML-Seite der Ausgabe.
Und dem Verarbeiten und Bereinigen der Stern-Artikel und Ausgaben (im JSON-Format) [### 3].

Erstellt von Matthias Endres fuer die Masterarbeit: "Von der Webseite zum Textkorpus – 
                                                    geschichtswissenschaftliches Web Scraping und digitale Datenmodellierung 
                                                    am Beispiel deutscher Nachrichtenmagazine"

Legende:

    ### X = Oberpunkte
    ## X  = Unterpunkte
    #     = Kommentare
    #!!!  = Zeilen fuer Debug-Zwecke (Entweder zum Herausloeschen oder Anfuegen.)
    #->   = lose Erlaeuterungen fuer die Faehigkeiten des Codes (Robustheit / Wiederverwendbarkeit / Effizienz / Wartbarkeit)

Weitere Anmerkungen:
    - Wenn moeglich, wurden alle Kommentare fuer die Lesbarkeit relativ kurz gehalten, sodass sie auch auf kleine Bildschirme passen.
    - Urspruenglich ausgefuehrt ueber: python 3.13.2 conda | VSCode
    - Datum der letzten Ausfuehrung: 03.06.2025
"""

### 0 - Imports

import requests
from bs4 import BeautifulSoup
import random
import time
import re
import os
import json
from datetime import datetime, timedelta
from html import unescape
import logging
from collections import defaultdict
import traceback, sys
import concurrent.futures
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import get_context


# Wichtig: Hier den Pfad festlegen, in welchem Verzeichnis alles angelegt wird. 
# Auf diesem Pfad basieren alle weiteren Verweise innerhalb der Funktionen.
# Sollte der Code auf einem anderen PC ausgefuehrt werden, oder sonst ein Rechnerwechsel passieren, wird es systementsprechend richtig geaendert. #-> Wiederverwendbarkeit (/Portierbarkeit)
base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Zuerst den Datenorder festlegen, in den die log-Datei gespeichert werden soll.
# Diese Art der Pfaderstellung sorgt dafuer, dass unabhaengig vom Betriebssystem die richtigen Trennzeichen gesetzt werden.
output_folder_local = os.path.join(base_path, "data")
    
# Die URL des Stern-Archivs fuer spaetere Verarbeitung festlegen.
url_stern_archive_default = "https://www.stern.de/archiv/"

# Sammeldatei erstellen, die alle nicht vorhandenen Webseiten auflistet.
incorrect_data_stern = {}

# Pfad erstellen.
incorrect_data_path_stern = os.path.join(output_folder_local, "stern_incorrect_data.json")


### 1 - Die Artikel-URLs herausfinden.

def crawl_stern_article_urls(url, output_folder, spec_log):
    """
    Alle Artikel-URLs herausbekommen.
        Parameter: Die URL des Stern-Archivs, der Ordner, in den es gespeichert wird, Log-Bedingung.
        Ausgabe: Keine.
    """

    # Fehlerprotokoll laden oder initialisieren.
    if os.path.exists(incorrect_data_path_stern):
        try:
            with open(incorrect_data_path_stern, "r", encoding="utf-8") as f:
                incorrect_data_stern = json.load(f)
        except json.JSONDecodeError:
            incorrect_data_stern = {}
    else:
        incorrect_data_stern = {}

    ## 1.0 Funktionen vordefinieren.

    def save_failed_url(url, indexing):
        """
        Speichert eine fehlgeschlagene URL in die Fehlerprotokoll-Datei. 
            Parameter: URL und Indexierung, die einfach der Lognachricht die richtige Nummerierung gibt. 
            Ausgabe: Keine.
        """

        if os.path.exists(failed_urls_file):
            # Liest bisherige Fehlerdatei aus (Falls sie Inhalt besitzt, heißt das, es gab einen Absturz, sonst waere sie ueberschieben worden).
            with open(failed_urls_file, "r", encoding="utf-8") as file:
                try:
                    failed_urls = json.load(file)
                except json.JSONDecodeError:
                    failed_urls = {}

            # Neue fehlerhafte Dateien in die Datei schreiben.
            failed_urls[url] = {"timestamp": datetime.now().isoformat()}
            with open(failed_urls_file, "w", encoding="utf-8") as file:
                json.dump(failed_urls, file, indent=4, ensure_ascii=False)
                
        # Falls Datei nicht existiert, sie erstellen und Daten einspeisen.
        else:
            failed_urls = {}
            failed_urls[url] = {"timestamp": datetime.now().isoformat()}
            with open(failed_urls_file, "w", encoding="utf-8") as file:
                json.dump(failed_urls, file, indent=4, ensure_ascii=False)
        logging.warning(f"{indexing} - Fehlerhafte URL gespeichert: {url}")

    def handle_rate_limit(pause_durations, attempt, indexing):
        """
        Verarbeite die Rate-Limit-Pausen-Logik basierend auf der aktuellen Anzahl der Versuche.
            Parameter 1: Die Minutenanzahl, die gewartet werden muss.
            Parameter 2: Die Anzahl der Versuche.
            Parameter 3: Indexierung.
            Ausgabe: Keine.
        """

        # Die Wartezeit abfragen.
        pause_time = pause_durations[min(attempt, len(pause_durations) -1)]
        logging.warning(f"{indexing} - Rate-Limiting erkannt. Warte {pause_time} Minuten.")

        # Pause in Minuten.
        time.sleep(pause_time * 60)

    def fetch_url(url, indexing, delay_range=(0.2, 0.8)):
        """
        URL-Inhalt mit zufaelliger Wartezeit holen.
            Parameter: Die abzurufende URL, Indexierung und der Rahmen der randomisierten Wartezeit.
            Ausgabe: Das Response-Objekt als Content, also in Bytes.
        """

        # Wartezeit einbauen.
        time.sleep(random.uniform(*delay_range))

        # Variablen fuer Versuchsanzahl und die Pausedauern einfuegen. #-> Robustheit #-> Effizienz
        attempt = 0
        pause_durations = [1, 2, 3, 4, 5, 10, 20, 30, 60, 120]

        # Solange der Versuch, die Website aufzurufen, noch nicht alle Wartezeiten durchprobiert hat, weitermachen.
        while attempt < len(pause_durations):
            try:
                response = requests.get(url, timeout=30)
                if 200 <= response.status_code <= 299:
                    if spec_log:
                        logging.info(f"{indexing} - Die URL [{url}] wurde erfolgreich aufgerufen.")
                    return response.content
                
                # Falls anderer Fehler kommt, kann die URL einfach ins Fehlerprotokoll uebernommen werden.
                else:
                    save_failed_url(url, indexing)
                    return None
                
            except requests.exceptions.Timeout:
                logging.error(f"{indexing} - Timeout bei der Anfrage an [{url}]. Versuch {attempt + 1}.")
                handle_rate_limit(pause_durations, attempt, indexing)
                attempt += 1

            except Exception as e:
                logging.error(f"{indexing} - Fehler beim Zugriff auf URL [{url}]: {e}")
                return None
            
        else:
            logging.error(f"{indexing} - Abbruch nach mehreren fehlgeschlagenen Versuchen fuer URL: [{url}].")
            save_failed_url(url, indexing)

    def get_category_urls(url, spec_log, indexing):
        """
        Alle URLs der einzelnen  Rubriken herausfinden.
            Parameter 1: Archiv-URL.
            Parameter 2: spec_log, Bedingung, die detailliertes Logging hinzufuegt.
            Parameter 3: indexing, Nummerierung fuer Log-Nachrichten.
            Ausgabe: Liste der Rubrik-URLs.
        """

        # Die URL anfragen.
        response = fetch_url(url, indexing)
        if spec_log:
                logging.debug(f"{indexing} - Archiv-URL erfolgreich aufgerufen.")

        # Nach den einzelnen Archiv-Websites suchen.
        soup = BeautifulSoup(response, "html.parser")
        pattern = re.compile(r"https://www\.stern\.de/[^/]+/archiv/")
        category_urls = set()

        # Die Links, welche in HTML immer in a-Tags sind, herausholen.
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            if pattern.match(href) and href not in excluded_urls:
                category_urls.add(href)
                if spec_log:
                    logging.debug(f"{indexing} - Rubrik [{href}] erfolgreich extrahiert.")
        
        logging.info(f"{indexing} - {len(category_urls)} Rubrik-URLs erfolgreich extrahiert.")

        return list(category_urls)
    
    def get_year_urls(category_url, spec_log, indexing):
        """"
        Alle URLs der einzelnen Jahre und Monate herausfinden.
            Parameter 1: category_url, Rubrik-URLs.
            Parameter 2: spec_log, Bedingung, die detailliertes Logging hinzufuegt.
            Parameter 3: indexing, Nummerierung fuer Log-Nachrichten.
            Ausgabe: Alle Jahre als Liste.
        """

        # Die URL anfragen und Response-Objekt.
        response = fetch_url(category_url, indexing)
        soup = BeautifulSoup(response, "html.parser")

        if spec_log:
                logging.debug(f"{indexing} - Rubrik-URLs erfolgreich aufgerufen.")

        # Jahre extrahieren.
        years_set = set()
        year_nav = soup.find("nav", {"aria-labelledby": "links-calendar--year"})
        if year_nav:
            for a_tag in year_nav.find_all("a", href=True):
                href = a_tag["href"]
                match = re.search(r"year=(\d{4})", href)
                if match:
                    years_set.add(match.group(1))
        
        years = sorted(list(years_set))

        if spec_log:
            logging.debug(f"{indexing} - Jahres-URL [{years}] erfolgreich aufgerufen.")

        logging.info(f"{indexing} - {len(years)} Jahre in Rubrik [{category_url}] gefunden.")

        return years
    
    def crawling_month_per_month(category, year, indexing, spec_log):
        """
        Sich von Monatsseite zu Monatsseite durchklicken. Wurde urspruenglich gemacht, um dynamische Seiten ohne große 
        Cache- Cookie- oder andere Header-Einstellungen miteinzuarbeiten. Dynamische Extraktion der einzelnen Monatsseiten hat nicht geklappt,
        aus Zeitgruenden wurde diese Struktur nun trotzdem beibehalten.
            Parameter 1: category, Die Rubriken, nach denen alles im Stern geordnet ist.
            Parameter 2: year, Das entsprechende Jahr der jeweiligen Kategorie.
            Parameter 3: indexing, Indexierung zur Nummerierung der Log-Nachrichten.
            Parameter 4: spec_log, Bedingung fuer detailliertes Logging.
            Ausgabe: months_urls, Alle Monats-URL.
        """

        visited = set()
        months_urls = []
        current_url = f"https://www.stern.de/{category}/archiv/?month=1&year={year}"

        # Alle noch nicht besuchten URLs (also alle neuen) durchsuchen.
        while current_url and current_url not in visited:
            visited.add(current_url)

            try:
                # Response-Objekt.
                response = fetch_url(current_url, indexing)
                soup = BeautifulSoup(response, "html.parser")
                months_urls.append(current_url)

                if spec_log:
                    logging.debug(f"{indexing} - Erfasst: {current_url}")

                # Vorher untersuchte Struktur abfragen.
                nav = soup.find("nav", {"aria-labelledby": "links-calendar--month"})
                if not nav:
                    break

                # Links finden.
                active_li = nav.find("a", class_="links-calendar__link u-button active")
                if not active_li:
                    break
                
                # Zum naechsten.
                next_li = active_li.find_parent("li").find_next_sibling("li")
                if next_li:
                    next_a = next_li.find("a", href=True)
                    if next_a:
                        next_url = next_a["href"]
                        if not next_url.startswith("http"):
                            next_url = requests.compat.urljoin(current_url, next_url)
                        current_url = next_url
                        time.sleep(0.5)
                        continue
                break

            except Exception as e:
                logging.error(f"{indexing} - Fehler beim Crawlen von [{current_url}]: {e}")
                break

        return months_urls
    
    def get_paginated_urls_per_month(category, year, month, spec_log, indexing):
        """
        Alle Seitenzahl-URLs der einzelnen Monate herausfinden.
            Parameter 1: Rubrik-String.
            Parameter 2: Jahres-String.
            Parameter 3: Monats-String.
            Parameter 4: spec_log, Bedingung, die detailliertes Logging hinzufuegt.
            Parameter 5: indexing, Nummerierung fuer Log-Nachrichten.
            Ausgabe: URLs der einzelnen Seiten der einzelnen Monate.
        """

        # URLs werden nun nicht aus dynamischem HTML gezogen, sondern kuenstlich nach dem gefundenen Muster erstellt.
        # Wuerde hier noch dynamisches HTML implementiert werden, waeren das alleine dafuer 6 Wochen Arbeitsaufwand, 
        # die einfach nicht gerechtfertigt sind, daher diese faule (effiziente) Loesung.
        base_url = f"https://www.stern.de/{category}/archiv/?month={month}&year={year}"

        # Abfrage URL und Soup-Objekt.
        response = fetch_url(base_url, indexing)
        soup = BeautifulSoup(response, "html.parser")

        # Um Wiederholungen direkt zu vermeiden, set einfuehren.
        paginated_urls = set()
        paginated_urls.add(base_url)

        # Die einzelnen Seiten der Monats-Website finden.
        pagination = soup.find("ul", class_="pagination__pages")
        if pagination:
            for a_tag in pagination.find_all("a", href=True):
                href = a_tag["href"]
                full_url = requests.compat.urljoin(base_url, href)
                paginated_urls.add(full_url)
                if spec_log:
                        logging.debug(f"{indexing} - Monatsseiten-URLs erfolgreich aufgerufen.")
        if spec_log:
            logging.debug(f"{indexing} - {len(paginated_urls)} paginierte Seiten fuer Monat {month}, Jahr {year} in Rubrik {category} gefunden.")

        return paginated_urls
    
    def extract_sort_key(url):
        """
        Sortierungsschluessel fuer JSON-Datei festlegen. Ausgaben als Tupel zurueckgeben.
            Parameter: jeweils gefragte URL.
            Ausgabe 1: year, entspechendes Jahr.
            Ausgabe 2: month, entsprechender Monat.
            Ausgabe 3: page, entsprechende Seite.
        """
        match_year = re.search(r"year=(\d{4})", url)
        match_month = re.search(r"month=(\d{1,2})", url)
        match_page = re.search(r"pageNum=(\d+)", url)

        year = int(match_year.group(1)) if match_year else 0
        month = int(match_month.group(1)) if match_month else 0
        page = int(match_page.group(1)) if match_page else 0  # Seiten ohne pageNum → Seite 0

        return (year, month, page)
    
    # Output-Ordner definieren.
    output_json_path = os.path.join(output_folder, "STERN_issues_and_articles_def_1.json")

    # Set vordefinieren.
    all_urls = set()

    # Ausnahmen definieren.
    excluded_urls = {"https://www.stern.de/noch-fragen/archiv/"}
    #!!! allowed_categories = "politik" #!!! Debug-Zwecke 1
    #!!! allowed_years = "2008" #!!! Debug-Zwecke 2
    #!!! allowed_months = "3"  #!!! Debug-Zwecke 3
    
    # Fehlerprotokoll - Dateipfad.
    failed_urls_file = os.path.join(output_folder, "failed_urls_stern_def_1.json")

    ## 1.1 - Rubriken extrahieren. 

    logging.info(f"1 - Extraktion der Stern-Artikel-URLs beginnt")
    category_urls = get_category_urls(url, spec_log=spec_log, indexing="1.1")

    ## 1.2 - Alle Monatsseiten-URLs extrahieren.

    # Jede Rubrik durchiterieren.
    for category_url in category_urls:

        ## 1.2.1 - Extrahiert den Rubriknamen.

        #!!! if allowed_categories and allowed_categories not in category_url: #!!! Debug-Zwecke 1
        #!!!     continue #!!! Debug-Zwecke 1
        years = get_year_urls(category_url, spec_log=spec_log, indexing="1.2.1")

        ## 1.2.2 Alle Websites, auf denen Artikel sind, extrahieren.

        # Alle Jahre durchiterieren.
        for year in years:
            #!!! if allowed_years and year != allowed_years: #!!! Debug-Zwecke 2
            #!!!      continue #!!! Debug-Zwecke 2

            # Alle Monate durchiterieren.
            # Rubrikenname extrahieren, notwendig fuer Funktion.
            category = category_url.split("/")[3]

            # Alle Monats-URLs "dynamisch" ermitteln.
            month_urls = crawling_month_per_month(category, year, indexing="1.2.2", spec_log=spec_log)

            # Dann fuer jede Monatsseite die paginierten URLs extrahieren.
            for month_url in month_urls:

                match = re.search(r"month=(\d{1,2})", month_url)
                month = match.group(1) if match else "1" 

                #!!! Debug-Zwecke 3: Nur bestimmte Monate zulassen
                #!!! if allowed_months and month not in allowed_months: #!!! Debug-Zwecke 3
                #!!!     continue #!!! Debug-Zwecke 3

                try:
                    paginated = get_paginated_urls_per_month(category, year, month, spec_log=spec_log, indexing="1.2.2")
                    all_urls.update(paginated)

                except Exception as e:
                    logging.error(f"1.2.2 - Fehler bei Monatsverarbeitung: {e}")

    ## 1.3 - Artikel-URLs pro Archiv-Seite extrahieren und speichern.

    # Dictionary, das spaeter die Struktur {seiten_url: [artikel_url1, artikel_url2, ...]} erhaelt.
    output_data = {}

    ## 1.3.1 - Bisherige URLs sortieren.

    # URLs vor der Verarbeitung sortieren (nach Jahr, Monat, Seite).
    try:
        sorted_urls = sorted(list(all_urls), key=extract_sort_key)
        if spec_log:
            logging.debug("1.3.1 - Monatsseiten URLs wurden sortiert.")
                    
    except UnicodeDecodeError as e:
        logging.error(f"1.3.1 - UniDecodeError fuer '{output_json_path}': [{e}]")

    except Exception as e:
        logging.error(f"1.3.1 - Fehler beim Schreiben der Datei '{output_json_path}': [{e}]")

    ## 1.3.2 - Artikellinks extrahieren.

    # Jede Seite durchgehen, HTML laden, Artikel-Links extrahieren.
    for page_url in sorted_urls:

        try:
            # HTML-Inhalt der Seite abrufen.
            html = fetch_url(page_url, indexing="1.3.2")
            if not html:
                # Wenn nichts geladen wurde (Fehler oder Timeout), leere Liste eintragen.
                output_data[page_url] = []
                continue

            # Seite parsen.
            soup = BeautifulSoup(html, "html.parser")

            # Alle Artikel sammeln, die zur Klasse 'teaser-plaintext' gehoeren.
            article_urls = set()
            for article in soup.find_all("article", class_="teaser-plaintext"):
                a_tag = article.find("a", href=True)
                if a_tag and a_tag["href"].startswith("http"):
                    article_urls.add(a_tag["href"])

            # Ergebnisse im Dictionary speichern.
            output_data[page_url] = list(article_urls)

            if spec_log:
                logging.info(f"1.3.2 - {len(article_urls)} Artikel auf Seite gefunden: {page_url}")

        except Exception as e:
            # Bei Fehlern auch leere Liste eintragen, damit keine Seite fehlt.
            output_data[page_url] = []
            logging.error(f"1.3.2 - Fehler bei Artikel-Extraktion von {page_url}: {e}")
        
    # Gesamtanzahl aller Artikel-Links berechnen (ueber alle Seiten hinweg).
    total_articles = sum(len(set(urls)) for urls in output_data.values())

    logging.info(f"1.3.2 - Insgesamt wurden {total_articles} Artikel-URLs gefunden.")
    print(f"1.3.2 - Insgesamt wurden {total_articles} Artikel-URLs gefunden.")

    # Final: JSON-Datei schreiben mit der neuen Dictionary-Struktur.
    with open(output_json_path, "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)
        logging.info(f"1.3.2 - Artikel-URLs wurden pro Seite extrahiert und in '{output_json_path}' gespeichert.")

    ## 1.4 - Fehlerprotokoll abspeichern.
    try:
        with open(incorrect_data_path_stern, "w", encoding="utf-8") as file:
            json.dump(incorrect_data_stern, file, indent=4, ensure_ascii=False)
        if spec_log:
            logging.info("1.4 - Fehlerprotokoll wurde erfolgreich gespeichert.")
    except Exception as e:
        logging.error(f"1.4 - Fehler beim Speichern des Fehlerprotokolls: {e}")


### 2 - Download der Stern-Inhalte

def download_articles_by_page_file_stern(output_folder, input_file, spec_log):
    """
    Laedt Artikel-HTML-Inhalte herunter und speichert sie in einer strukturierten Ordnerstruktur.
    Struktur: www.stern.de/stern_archiv/<rubrik>/<rubrik>_<jahr>/<rubrik>_<jahr>_<monat>/
                                     /<rubrik>_<jahr>_<monat>_pageX/<artikel>.html
        Parameter 1: Zielordner fuer die Ausgabe.
        Parameter 2: Pfad zur Datei mit Monats-/Seiten-URLs und Artikel-URLs (JSON-Struktur).
        Parameter 3: Log-Bedingung.
        Ausgabe: Keine.
    """

    logging.info("2.0 - Download der Rohdaten beginnt nun.")

    if not os.path.exists(input_file):
        logging.error(f"2.0 - Die Datei [{input_file}] wurde nicht gefunden.")
        print(f"Die Datei [{input_file}] wurde nicht gefunden.")
        return

    # Datei auslesen, um URLs zu extrahieren.
    with open(input_file, "r", encoding="utf-8") as file:
        url_data = json.load(file)

    if spec_log:
        logging.info(f"2.0 - {len(url_data)} Seiten-URLs aus der Datei [{input_file}] geladen.")

    # Ausgabeordner erstellen, falls es sie noch nicht gibt.
    os.makedirs(output_folder, exist_ok=True)
    base_folder = os.path.join(output_folder, "www.stern.de", "stern_archiv")
    os.makedirs(base_folder, exist_ok=True)

    failed_articles_file = os.path.join(output_folder, "failed_articles_stern_def_2.json")
    failed_articles = {}

    # Fehlerprotokoll laden.
    if os.path.exists(failed_articles_file):
        with open(failed_articles_file, "r", encoding="utf-8") as file:
            failed_articles = json.load(file)

    # Pausen fuer Ratelimiting.
    pause_durations = [1, 2, 3, 4, 5, 10, 20, 30, 60, 120]

    def handle_rate_limit(pause_durations, attempt, indexing):
        """
        Verarbeite die Rate-Limit-Pausen-Logik basierend auf der aktuellen Anzahl der Versuche.
            Parameter 1: Die Minutenanzahl, die gewartet werden muss.
            Parameter 2: Die Anzahl der Versuche.
            Parameter 3: Indexierung.
            Ausgabe: Keine.
        """

        pause_time = pause_durations[min(attempt, len(pause_durations) - 1)]
        logging.warning(f"{indexing} - Rate-Limiting erkannt. Warte {pause_time} Minuten.")

        # Tatsaechliche Wartezeit ausfuehren.
        time.sleep(pause_time * 60)

    def parse_url_parts(url):
        """
        Die in der URL vorhandenen Informationen extrahieren.
            Parameter: Eben die URL.
            Ausgabe 1: category, die Rubrik.
            Ausgabe 2: year, das Jahr.
            Ausgabe 3: month, der Monat.
            Ausgabe 4: page, die Seite, da ja die meisten Monate so viele Artikel haben, dass es mehrere paginierte Seiten gibt.
        """

        # Suchmuster.
        match = re.search(r"stern.de/([^/]+)/archiv/\?month=(\d{1,2})&year=(\d{4})(?:&pageNum=(\d+))?", url)
        
        if match:
            category, month, year, page = match.groups()
            month = month.zfill(2)
            page = page or "0"
            return category, year, month, page
        
        # Falls eben kein Muster gefunden wird, alles auf None setzen.
        return None, None, None, None

    # Session einrichten.
    with requests.Session() as session:

        # Alle URLs aus Liste holen und fuer jede einzelne das entsprechende Verzeichnis anlegen.
        for parent_url, article_urls in url_data.items():

            category, year, month, page = parse_url_parts(parent_url)
            if not all([category, year, month, page]):
                continue

            folder_path = os.path.join(
                base_folder,
                category,
                f"{category}_{year}",
                f"{category}_{year}_{month}",
                f"{category}_{year}_{month}_page{page}"
            )
            os.makedirs(folder_path, exist_ok=True)

            # Die Dateinamen anlegen, dass sie auch Windows verarbeitet.
            for article_url in set(article_urls):

                max_path_length = 255
                suffix = ".html"
                base_name = article_url.split("/")[-1].split("?")[0]
                max_name_length = max_path_length - len(suffix) - len(folder_path) - 5

                if base_name.endswith(suffix):
                    article_html_file = base_name[:max_name_length]
                else:
                    article_html_file = base_name[:max_name_length] + suffix

                # Pfad anlegen.
                article_path = os.path.join(folder_path, article_html_file)

                if os.path.exists(article_path): # Ueberschreibschutz.
                    if spec_log:
                        logging.debug(f"2.1.2 - Artikel [{article_url}] existiert bereits. Ueberspringe.")
                    continue

                # Nun tatsaechlich gesamten HTML-Inhalt herunterladen.
                attempt = 0

                while attempt < len(pause_durations):
                    try:

                        if spec_log:
                            logging.info(f"2.1.2 - Rufe Artikel ab: [{article_url}]")

                        # Wartezeit einfuegen.
                        delay = random.uniform(0.2, 0.8) 
                        time.sleep(delay)
                        if spec_log:
                            logging.debug(f"Wartezeit vor Anfrage: {delay:.2f} Sekunden")

                        response = session.get(article_url, timeout=30)
                        # Hier den Status-Code auf 200 setzen, im Gegensatz zu ## 1.3, ## 1.4 und ## 1.5, da es hier von Relevanz sein koennte.
                        if response.status_code == 200:
                            html_text = response.content.decode("utf-8", errors="replace")
                            with open(article_path, "w", encoding="utf-8") as file:
                                file.write(html_text)
                            if spec_log:
                                logging.debug(f"2.1.2 - Artikel [{article_url}] HTML gespeichert.")
                            break

                        else:
                            logging.warning(f"2.1.2 - Fehlerhafte Artikel-URL: [{article_url}] mit Status [{response.status_code}]")
                            failed_articles[article_url] = {
                                "timestamp": datetime.now().isoformat(),
                                "status_code": response.status_code
                            }
                            break

                    except requests.exceptions.Timeout:
                        logging.error(f"2.1.2 - Timeout bei der Anfrage an [{article_url}]. Versuch {attempt + 1}.")
                        handle_rate_limit(pause_durations, attempt, "2.1.2")
                        attempt += 1

                    
                    except Exception as e: #-> Wartbarkeit
                        logging.error(f"2.1.1 - Fehler beim Abrufen des Artikels [{article_url}]: {e}")
                        failed_articles[article_url] = {
                            "timestamp": datetime.now().isoformat(),
                            "error": str(e)
                        }
                        break

                else:
                    logging.error(f"2.1.1 - Abbruch nach mehreren fehlgeschlagenen Versuchen fuer Artikel [{article_url}].")
                    failed_articles[article_url] = {
                        "timestamp": datetime.now().isoformat(),
                        "error": "Mehrere fehlgeschlagene Timeout-Versuche"
                    }
                    continue
            else:
                if spec_log:
                    logging.debug(f"2.1.1 - Artikel [{article_url}] existiert bereits. Ueberspringe.")
                pass
    
    # Fehlerprotokolle speichern. #-> Wartbarkeit
    with open(failed_articles_file, "w", encoding="utf-8") as file:
        json.dump(failed_articles, file, indent=4, ensure_ascii=False)


### 3 - Artikeldaten und Metadaten parallelisiert extrahieren.

## 3.0 - Funktionen vordefinieren.

def extract_article_metadata_stern(parent_url, article_file_input, folder_path, spec_log, indexing):
    """
    Extrahiert Daten und Metadaten aus einem Stern-Artikel-HTML. Um das auch wirklich noch vor Beendigung der Arbeit
    hinzubekommen, werden einige  Breakpoints und Versuchsbloecke herausgelassen, da ich weiß, dass es auch so funktioniert und
    das nur sehr sehr viele weitere Debug-Stunden kosten wuerde.
        Parameter 1: parent_url, URL der paginierten Seite zum Erzeugen der Pfade.
        Parameter 2: article_file_input, die HTML-Datei
        Parameter 3: folder_path, Pfad zum Seitenordner, in dem sich die Artikel befinden. Falls None, wird er aus der URL rekonstruiert.
        Parameter 4: spec_log, Bedingung, die detailliertes Logging hinzufuegt.
        Parameter 5: indexing, Nummerierung fuer Log-Nachrichten.
        Ausgabe 1: article_data_dict, strukturierte Daten und Metadaten.
        Ausgabe 2: parent_url, Die Ausgangs-URL der paginierten Seite (zur Wiederzuordnung).
        Ausgabe 3: article_key, Eindeutiger Artikelbezeichner im Format "article - 001-090".
        Ausgabe 4: incorrect_data_stern, Fehlerhafte oder unlesbare Artikeldateien fuer das Fehlerprotokoll.
    """

    # Ordnerpfad aus parent_url erzeugen (nur wenn folder_path nicht explizit gesetzt ist).
    if not folder_path:
        match_year = re.search(r"year=(\d{4})", parent_url)
        match_month = re.search(r"month=(\d{1,2})", parent_url)
        match_page = re.search(r"pageNum=(\d+)", parent_url)
        match_category = re.search(r"www\.stern\.de/([^/]+)/", parent_url)

        year = match_year.group(1) if match_year else "0000"
        month = match_month.group(1).zfill(2) if match_month else "01"
        page = f"page{match_page.group(1)}" if match_page else "page1"
        category = match_category.group(1) if match_category else "unknown"

        folder_path = os.path.join(
            "data", "www.stern.de", "stern_archiv",
            category,
            f"{category}_{year}",
            f"{category}_{year}_{month}",
            f"{category}_{year}_{month}_{page}"
        )

    # Variablen vordefinieren.
    article_data_dict = {}
    incorrect_data_stern = {}
    article_key = "article - Null"

    article_path = os.path.join(folder_path, article_file_input)

    if not os.path.exists(article_path):
        incorrect_data_stern.setdefault(parent_url, {})[article_file_input] = "Datei nicht gefunden."
        return {}, parent_url, article_key, incorrect_data_stern

    try:
        # Datei auslesen.
        with open(article_path, "r", encoding="utf-8") as f:
            soup = BeautifulSoup(f, "html.parser")

        # 1. Artikelnummer generieren (aus Dateiname).
        # Pfad des Ordners, in dem sich die Artikel befinden.
        page_folder_path = os.path.dirname(article_path)
        page_folder_name = os.path.basename(page_folder_path)

        # Seitenzahl aus dem Ordnernamen extrahieren (z. B. "page0" → 0).
        page_number_match = re.search(r"page(\d+)", page_folder_name)
        page_number = page_number_match.group(1) if page_number_match else "0"

        # Alle Artikel-Dateien (ohne index.html), alphabetisch sortieren.
        files_sorted = sorted(
            f for f in os.listdir(page_folder_path)
            if f.endswith(".html") and f != "index.html"
        )

        # Position des aktuellen Artikels ermitteln.
        article_index = files_sorted.index(article_file_input) + 1
        article_total = len(files_sorted)

        # Artikelnummer zusammensetzen.
        article_number = f"{str(article_index).zfill(3)}-{str(article_total).zfill(3)}"

        # 2. Titel extrahieren.
        meta_title = soup.find("meta", attrs={"name": "ob_headline"})
        article_title = meta_title["content"].strip() if meta_title else None

        # 3. Untertitel = None.
        article_subtitle = None
        text_blocks_subtitle = []
        for tag in soup.select("div.intro.typo-intro.u-richtext"):
            text_blocks_subtitle.append(str(tag))

        article_subtitle_with_html = "".join(text_blocks_subtitle) if text_blocks_subtitle else None
        article_subtitle = BeautifulSoup(article_subtitle_with_html or "", "html.parser").get_text(" ")

        # 4. Dachzeile (Kicker) extrahieren.
        kickers = []
        meta_kicker = soup.find("meta", attrs={"name": "ob_kicker"})
        if meta_kicker:
            kickers.append(meta_kicker["content"].strip())
        title_tag = soup.find("title")
        if title_tag:
            raw_title = title_tag.text.replace("| STERN.de", "").strip()
            kickers.append(unescape(raw_title))
        article_kicker = kickers if kickers else None

        # 5. URL extrahieren.
        link_tag = soup.find("link", rel="canonical")
        article_url = link_tag["href"].strip() if link_tag else None

        # 6. Veroeffentlichungsdatum extrahieren.
        article_publication_date = None
        pub_date_meta = soup.find("meta", attrs={"name": "last-modified"})
        if pub_date_meta and pub_date_meta.has_attr("content"):
            try:
                article_publication_date = datetime.fromisoformat(pub_date_meta["content"])
            except ValueError:
                if spec_log:
                    logging.warning(f"{indexing} - Ungueltiges Datum: {pub_date_meta['content']}")

        # Zeitzonen-Offset entfernen.
        if article_publication_date.tzinfo is not None:
            article_publication_date = article_publication_date.replace(tzinfo=None)

        # 7. Autoren extrahieren.
        authors = []
        byline_block = soup.find("div", class_="authors typo-article-info")
        if byline_block:
            name_links = byline_block.find_all("a", class_="authors__list-link")
            for link in name_links:
                authors.append(link.get_text(strip=True))
            for bold in byline_block.find_all("span", class_="typo-article-info-bold"):
                authors.append(bold.get_text(strip=True))
        alt_author_blocks = soup.find_all("div", class_=re.compile(r"authors__original-source|authors__shortcode"))
        for block in alt_author_blocks:
            authors.append(block.get_text(strip=True))
        article_authors = list(filter(None, authors)) or None

        # 8. Rubrik & 9. Keywords aus JSON im <ws-gtm>.
        article_category = []
        keywords = []
        gtm_script = soup.find("ws-gtm")
        if gtm_script:
            json_tag = gtm_script.find("script", type="application/json")
            if json_tag:
                try:
                    gtm_data = json.loads(json_tag.string)
                    content = gtm_data.get("content", {})
                    if content:
                        article_category.append(content.get("main_section"))
                        for key in ["sub_section_1", "sub_section_2", "sub_section_3"]:
                            val = content.get(key)
                            if val and val != "not_set":
                                article_category.append(val)
                        kws = content.get("ad_keywords", "")
                        keywords = [kw.strip() for kw in kws.split(",") if kw.strip() not in ["stern", "ct_article", "onecore"]]
                        date_of_last_update = content.get("last_update_date")
                except json.JSONDecodeError:
                    pass

        article_category = list(filter(None, article_category)) or None
        keywords = keywords or None

        # 10 & 11. Lesedauer extrahieren.
        reading_time = None
        is_reading_time = False
        meta_data_list = soup.select("ul.authors__meta-data.u-blanklist li")
        for li in meta_data_list:
            txt = li.get_text(strip=True)
            if txt.endswith("Min"):
                reading_time = txt
                is_reading_time = True
                break

        # 12. Urheberrecht errechnen.
        copyright_bool = None
        if article_publication_date:
            date_today = datetime.today()
            date_70_years_ago = date_today - timedelta(days=70*365.25)
            copyright_bool = article_publication_date > date_70_years_ago

        # 13-15. Fixe Werte.
        is_paywall = False
        is_comment = False
        is_button_like = False

        # 16. Merkliste extrahieren.
        is_button_save = bool(soup.find("i", class_="icon-bookmark"))
        is_button_copy_link = bool(soup.find("i", class_="icon-link"))

        # 18-20. Weitere fixe Werte.
        is_button_send_email = False
        platforms_sharing = []
        is_advertisement = True

        # 21. last_update extrahieren.
        date_of_last_update = date_of_last_update if "date_of_last_update" in locals() else None

        # 22-24. Text extrahieren.
        text_blocks = []
        for tag in soup.select(" div.text-element.u-richtext, h2.subheadline-element"):
            text_blocks.append(str(tag))

        article_text = "".join(text_blocks) if text_blocks else None
        text_plain = BeautifulSoup(article_text or "", "html.parser").get_text(" ")
        word_count = len(text_plain.split())
        character_count_with_whitespaces = len(text_plain)

        article_key = f"article - {article_number}"

        # Hier muss kein Schluessel fuer das spaetere JSON-Element festgelegt werden.
        # Ueberspringe fehlerhafte Artikel und speichere sie ab (Zielt weniger auf leere Artikel ab und mehr auf falschen html-Code).
        if not all([article_title, article_url, article_text]):

            if os.path.exists(incorrect_data_path_stern):
                with open(incorrect_data_path_stern, "r", encoding="utf-8") as f:
                    try:
                        incorrect_data_spiegel = json.load(f)
                    except json.JSONDecodeError:
                        incorrect_data_spiegel = {}
            else:
                incorrect_data_spiegel = {}

            logging.warning(f"{indexing} - Ueberspringe Artikel [{article_file_input}]: Fehlende Daten - Title: {article_title}, URL: {article_url}, Text Laenge: {len(article_text) if article_text else 0}")
            key_for_error_naming = f"{article_title} ({article_file_input})" if article_title else article_file_input
            incorrect_data_spiegel.setdefault(parent_url, {}).setdefault("article_data_dict", {})[key_for_error_naming] = f"Fehlende oder fehlerhafte Artikeldaten. Siehe: [{article_url}]"


        # Daten und Metadaten zusammensetzen.
        article_data_dict[article_key] = {
            "article_title": article_title,
            "article_subtitle": article_subtitle,
            "article_kicker": article_kicker,
            "article_number": article_number,
            "article_url": article_url,
            "article_publication_date": article_publication_date.isoformat() if article_publication_date else None, # Um es als string weiterzugeben und nicht als datetime-Objekt zu verarbeiten.
            "author": article_authors,
            "article_category": article_category,
            "keywords": keywords,
            "is_reading_time": is_reading_time,
            "reading_time": reading_time,
            "is_copyright": copyright_bool,
            "is_paywall": is_paywall,
            "is_comment": is_comment,
            "is_button_like": is_button_like,
            "is_button_save": is_button_save,
            "is_button_copy_link": is_button_copy_link,
            "is_button_send_email": is_button_send_email,
            "platforms_sharing": platforms_sharing,
            "is_advertisement": is_advertisement,
            "date_of_last_update": date_of_last_update,
            "word_count": word_count,
            "character_count_with_whitespaces": character_count_with_whitespaces,
            "article_text": article_text
        }

        # Fuege Logging fuer Artikel hinzu.
        if spec_log:
            logging.debug(f"{indexing} - Extrahierter Artikel: [{article_file_input}]")

    except FileNotFoundError as e:
        logging.error(f"{indexing} - Datei nicht gefunden [{article_file_input}]: {e}")
        # Damit bei einer fehlerhaften Artikelverarbeitung nicht alles gestoppt wird, muss es weiterhin eine Rueckgabe geben.
        incorrect_data_stern.setdefault(parent_url, {}).setdefault("article_data_dict", {})[article_file_input] = f"Fehler: {str(e)}"
        return {}, parent_url, article_key, incorrect_data_stern
    
    except UnicodeDecodeError as e:
        logging.error(f"{indexing} - Encoding-Fehler in Datei [{article_file_input}]: {e}")
        incorrect_data_stern.setdefault(parent_url, {}).setdefault("article_data_dict", {})[article_file_input] = f"Fehler: {str(e)}"
        return {}, parent_url, article_key, incorrect_data_stern
    
    except Exception as e:
        logging.error(f"{indexing} - Unerwarteter Fehler beim Verarbeiten von [{article_file_input}]: {e}")
        incorrect_data_stern.setdefault(parent_url, {}).setdefault("article_data_dict", {})[article_file_input] = f"Fehler: {str(e)}"
        return {}, parent_url, article_key, incorrect_data_stern

    if spec_log:
        logging.debug(f"{indexing} - Extrahierte Artikel fuer [{parent_url}]: {article_data_dict.keys()}")


    return article_data_dict, parent_url, article_key, incorrect_data_stern


def add_file_size_to_output_file_stern(output_directory_input, spec_log, indexing):
    """
    Fuegt in jeder JSON-Datei im gegebenen Verzeichnis die Dateigroeße (in KB) in den Block "general_metadata" ein.
        Parameter 1: output_directory_input, Pfad zum Verzeichnis mit den Jahres-JSON-Dateien (z. B. "stern-2019.json").
        Parameter 2: spec_log, Aktiviert ausfuehrliches Logging.
        Parameter 3: indexing, Logging-Nummerierung.
        Ausgabe: Keine.
    """

    for filename in (f for f in os.listdir(output_directory_input) if re.match(r"^stern-\d{4}\.json$", f, re.IGNORECASE)):
        try:
            file_path = os.path.join(output_directory_input, filename)
            output_file_size_kb = os.path.getsize(file_path) // 1024

            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            stern_key = next(iter(data))
            data[stern_key]["general_metadata"]["file_size_in_kibibyte"] = output_file_size_kb

            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            print(f"Dateigroeße hinzugefuegt: {file_path}")
            if spec_log:
                logging.info(f"{indexing} - Dateigroeße zu [{file_path}] hinzugefuegt.")

        except (json.JSONDecodeError, KeyError, OSError) as e:
            logging.error(f"{indexing} - Fehler bei [{filename}]: {e}")

def get_year_wise_input_data(input_directory, spec_log=False):
    """
    Durchsucht das HTML-Verzeichnis rekursiv und extrahiert Pfade zu allen paginierten Seiten.
        Parameter 1: input_directory, Wurzelverzeichnis mit lokal gespeicherten HTML-Daten des Stern.
        Parameter 2: spec_log, Bedingung fuer detailliertes Logging.
        Ausgabe: dict(all_data), Dictionary, siehe Beispiel.
    Beispiel:
    {
        "2019": {
            "03": {
                "politik_2019_03_page0": "/path/to/seite",
                ...
            }
        }
    }
    """
    all_data = defaultdict(lambda: defaultdict(dict))

    for root, dirs, _ in os.walk(input_directory):
        for dir_name in dirs:
            match = re.match(r"(?P<category>\w+)_(?P<year>\d{4})_(?P<month>\d{2})_page(?P<page_num>\d+)", dir_name)
            if match:
                info = match.groupdict()
                year = info["year"]
                month = info["month"]
                full_path = os.path.join(root, dir_name)
                all_data[year][month][dir_name] = full_path
                if spec_log:
                    logging.debug(f"Verzeichnis erkannt: {dir_name} → {full_path}")

    return dict(all_data)

def build_worktable_articles_stern(page_entry):
    """
    Nimmt eine Seite (aus build_worktable_pages_stern) und erzeugt die Artikelverarbeitungseintraege.
        Parameter: page_entry, tuple aus (page_url, page_value, folder_path, page_html_path, category).
        Ausgabe: worktable, Liste von Tuplen mit Artikeldaten fuer die Parallelverarbeitung: parent_url, article_file_input, folder_path, spec_log, indexing.
    """
    page_url, page_value, folder_path, page_html_path, category = page_entry
    worktable = []

    try:
        if not os.path.exists(folder_path):
            return []

        files = [f for f in os.listdir(folder_path) if f.endswith(".html") and f != "index.html"]

        for article_file in files:
            worktable.append((
                page_url,            # parent_url.
                article_file,        # article_file_input.
                folder_path,         # folder_path.
                False,               # spec_log.
                "3.5"                # indexing.
            ))
    except Exception as e:
        logging.warning(f"Fehler beim Bauen des Worktable-Eintrags fuer {folder_path}: {e}")
        return []

    return worktable

def build_worktable_pages_stern(page_paths, spec_log, indexing):
    """
    Generator, der die URL und Basis-Metadaten (category, year, month, page) jeder paginierten Seite vorbereitet.
    Diese Version erwartet page_paths im Format:
    {ordnername: pfad_zur_seite} – also fuer einen einzelnen Monat.
        Parameter 1: page_paths, Dict im Format {ordnername: pfad_zur_seite}.
        Parameter 2: spec_log, Bedingung, die detailliertes Logging hinzufuegt.
        Parameter 3: indexing, Nummerierung fuer Log-Nachrichten.
    Ausgaben werden mit yield abgerufen.
        Ausgabe 1: page_url, finale URL dieser Seite (fuer JSON-Key in Hauptdatei).
        Ausgabe 1: page_value, Dict mit Basisinformationen + leerem "article" dict.
        Ausgabe 2: page_folder_path, Pfad zum Ordner dieser Seite.
        Ausgabe 3: page_html_path, index.html-Dateipfad.
        Ausgabe 4: category, Ressort der Seite (z. B. politik).
    """

    for folder_name, folder_path in page_paths.items():
        try:
            # Beispiel fuer folder_name: politik_2019_06_page1.
            match = re.match(r"(?P<category>\w+)_(?P<year>\d{4})_(?P<month>\d{2})_page(?P<page_num>\d+)", folder_name)
            if not match:
                if spec_log:
                    logging.warning(f"{indexing} - Ordnername [{folder_name}] nicht im erwarteten Format.")
                continue

            info = match.groupdict()
            category = info["category"]
            year = info["year"]
            month = info["month"]
            page_num = int(info["page_num"])

            # URL zusammensetzen.
            base_url = f"https://www.stern.de/{category}/archiv/?month={int(month)}&year={year}"
            page_url = f"{base_url}&pageNum={page_num}" if page_num > 0 else base_url

            # page_value erzeugen.
            page_value = {
                "category": category,
                "year": year,
                "month": month,
                "page": f"{page_num:02d}",
                "article": {}
            }

            # index.html dieser Seite.
            page_html_path = os.path.join(folder_path, "index.html")

            yield page_url, page_value, folder_path, page_html_path, category

        except Exception as e:
            logging.error(f"{indexing} - Fehler beim Verarbeiten von Ordner [{folder_name}]: {e}")
            continue

## 3.5 Parallelisierung Stack-Trace-Analyse. #-> Wartbarkeit

def full_stack():
    """
    Detaillierte Fehleranalyse ausgegeben bekommen, falls die Parallelisierung schief geht.
        Parameter: Keine.
        Ausgabe: Stackstring.
    """
    exc = sys.exc_info()[0]

    if exc is not None:
        f = sys.exc_info()[-1].tb_frame.f_back
        stack = traceback.extract_stack(f)
    else:
        stack = traceback.extract_stack()[:-1]

    trc = "Traceback (most recent call last):\n"
    stackstr = trc + "".join(traceback.format_list(stack))

    if exc is not None:
        stackstr += "  " + traceback.format_exc().lstrip(trc)

    return stackstr

# 3.1 Die Daten werden nun bereinigt und zu JSON-Dateien verarbeitet.

def extract_stern_data(input_directory, output_directory, output_folder, spec_log):
    """
    Extrahiert alle Stern-Artikel- und Seitenmetadaten ueber Parallelisierung.
        Parameter 1: input_directory, Verzeichnis der HTML-Daten
        Parameter 2: output_directory, Zielverzeichnis fuer fertige JSON-Dateien
        Parameter 3: output_folder, globaler data-Ordner
        Parameter 4: spec_log, erweiterte Log-Ausgaben
        Ausgabe: output_directory, der Pfad der finalen JSON-Dateien.
    """

    logging.debug(f"3.1 - Starte Extraktion in [{input_directory}], speichere nach [{output_directory}]")
    os.makedirs(output_folder, exist_ok=True)
    os.makedirs(output_directory, exist_ok=True)

    # Warum hab ich das in der Funktion geschrieben? Ach egal, das ist sogar noch einfacher, als beim Spiegel
    # und dadurch einfacher handhabbar.
    incorrect_data_path = os.path.join(output_folder, "stern_incorrect_data.json")

    ## 3.2 - Nochmals die Fehlerprotokolle ueberpruefen.

    # Fehlerprotokoll vorbereiten
    incorrect_data_stern = {}
    if os.path.exists(incorrect_data_path):
        try:
            with open(incorrect_data_path, "r", encoding="utf-8") as f:
                incorrect_data_stern = json.load(f)

        except json.JSONDecodeError:
            logging.error(f"3.2 - JSON-Fehler in [{incorrect_data_path}], erstelle neue Datei.")
            incorrect_data_stern = {}

        except Exception as e:
            logging.warning(f"3.2 - Fehler beim Laden des Fehlerprotokolls: {e}")
            incorrect_data_stern = {}

    # Sicherstellen, dass `incorrect_data` gueltiges JSON bleibt und auf den vorher schon vorgegebenen Pfad speichern. #-> Robustheit
    try:
        with open(incorrect_data_path, "w", encoding="utf-8") as incorrect_file:
            json.dump(incorrect_data_stern, incorrect_file, indent=4, ensure_ascii=False)

            if spec_log:
                logging.info(f"3.2 - Datei fuer fehlerhafte Daten [{incorrect_data_path}] wurde erfolgreich angelegt.")

    except Exception as e:
        logging.error(f"3.2 - Fehler beim Speichern der fehlerhaften Daten: [{e}]")

    ## 3.3 - Grundsaetzliche Variablen und Dateien festlegen.

    # Alle vorhandenen JSON-Dateien als Schutz gegen ueberschreiben. EInfach drin lassen, ich versteh es auch nicht.
    existing_files = [f for f in os.listdir(output_directory) if f.endswith(".json")]

    all_page_paths = get_year_wise_input_data(input_directory, spec_log=False)

    ## 3.4 - Die Metadaten der Ausgaben extrahieren und zwischenspeichern (Zeiteffizient, statt RAM-effizient, wie vom Erstbetreuer gefordert.). #-> Effizienz

    with get_context("spawn").Pool() as pool:

        for year, months_dict in all_page_paths.items():

            year_output_path = os.path.join(output_directory, f"stern-{year}.json")

            if os.path.exists(year_output_path):
                logging.warning(f"3.4 - Datei [{year_output_path}] existiert bereits. Ueberspringe Jahrgang {year}.")
                continue
            logging.info(f"3.4 - Aggregieren von Jahrgang {year}")

            general_metadata = {
                "data_scraping_date": datetime.today().strftime("%Y-%m-%d"),
                "scraper_name": "Matthias Endres",
                "institution_name": "Humboldt-Universitaet zu Berlin",
                "supervisor_name_primary": "Torsten Hiltmann",
                "supervisor_name_secondary": "Carolin Odebrecht",
                "notes_on_general_data_en": "author: Is no author mentioned, usually a news agency is mentioned instead. Sometimes the text type is also stated in this data. || category: Unlike in the dataset of Der Spiegel, the articles here would normally be organized by category-year-month-page. To maintain a certain degree of consistency, it is ordered chronologically, so the year is the primary file name, followed by a month-category-page hierarchy. || word_count: Every whitespace and every punctuation mark is used as a seperator. So it isn't very accurate. For example: The german number '500.000' (english: 500,000) is counted as '500' and '000', so as two words. || is_copyright: It is the german copyright.",
                "notes_on_general_data_de": "author: Sind keine Autoren direkt genannt, werden meist Nachrichtenagenturen genannt. Manchmal steht hier auch die Textsorte dabei. || category: Im Gegensatz zum Spiegel wären die Artikel hier normalerweise nach Rubrik-Jahr-Monat-Seite gegliedert. Um ein gewisses Maß an Einheitlichkeit zu haben, wurde es chronologisch geordnet, also ist das Jahr ausschlaggebend für die Datei, danach folgt die Hierarchie Monat-Rubrik-Seite (month-category-page). || word_count: Jedes Leerzeichen und Satzzeichen werden als Trennzeichen verwendet. Daher ist die Zaehlung nicht sehr akkurat. Zum Beispiel: Die Zahl '500.000' wird als '500' und '000' gezaehlt, also als zwei Woerter. || is_copyright: Es handelt sich um deutsches Urheberrecht.",
                "notes_on_specific_data_en": "Especially in the early years of the Stern archive there are few articles. It is not known whether this was due to the small scale of Stern Online at the time.",
                "notes_on_specific_data_de": "Vor allem in den ersten Jahren des Stern-Archivs gibt es wenige Artikel. Ob das am damaligen geringen Umfang von Stern Online liegt, ist nicht bekannt.",
                "file_size_in_kibibyte": None
            }

            # Dictionary fuer das Jahr.
            year_data_stern = {f"Stern - {year}": {"general_metadata": general_metadata}}

            # Jetzt fuer jeden Monat einzeln.
            for month, month_page_paths in months_dict.items():
                page_list = list(build_worktable_pages_stern(month_page_paths, spec_log=False, indexing="3.4"))
                dict_page_url_value = dict([(page[0], page[1]) for page in page_list])

                ## 3.5 - Parallelisierung der Artikeldaten und Metadaten. #-> Effizienz

                try:
                    logging.info(f"3.5 - Parallelisierung der Artikel beginnt.")
                    
                    # Wurde bei der Durchfuehrung des Codes von Uniseite geaendert. Da es funktioniert werde ich einen Teufel tun und das wieder zu rein multiprocessing umaendern.
                    # Wirkt nicht sehr elegant, weil zwei Pools eroeffnet werden. Aber der Lehrstuhl wird das besser koennen, als ich.
                    # Einen Prozesspool mit 96 Prozessen erstellen --> Auch moeglich, einfach alle Prozessoren, außer 4 oder wie viele auch immer zu nutzen. #-> Wiederverwendbarkeit (/Portierbarkeit)
                    total_cores = os.cpu_count() #!!! Debug-Zwecke 4: spaeter wieder auskommentieren.
                    usable_cores = max(1, total_cores - 2) #!!! Debug-Zwecke 4: spaeter wieder auskommentieren.
                    # executer = ProcessPoolExecutor(96) #!!! Debug-Zwecke 5:  das hier wieder einfuegen fuer normalen Lauf (Wenn es denn 96 Prozesse gibt. Da muss man gucken.): executer = ProcessPoolExecutor(96)
                    executer = ProcessPoolExecutor(usable_cores) #!!! Debug-Zwecke 4

                    page_list = map(list, page_list)
                    worktable, _ = concurrent.futures.wait([executer.submit(build_worktable_articles_stern, i) for i in page_list])
                    worktable = [y for x in worktable for y in x.result()]

                    results_parallelization = pool.starmap(extract_article_metadata_stern, worktable)

                except Exception as e:
                    logging.critical(f"3.5 - Schwerwiegender Fehler in der Parallelverarbeitung: {e}")
                    logging.critical(full_stack())
                    results_parallelization = []

                logging.info(f"3.5 - Parallelisierung der Artikel endet.")

                ## 3.6 - Die Daten den JSON-Dateien richtig uebergeben.

                for article_data_dict, page_url, article_key, local_errors in results_parallelization:
                    for k, v in local_errors.items():
                        incorrect_data_stern.setdefault(k, {}).update(v)

                    if not article_data_dict:
                        logging.warning(f"3.5 - Leerer Artikel auf Seite [{page_url}] gefunden.")
                        incorrect_data_stern.setdefault(page_url, {})["Article"] = "Leerer Artikel gefunden."
                        continue

                    if "article" not in dict_page_url_value[page_url]:
                        dict_page_url_value[page_url]["article"] = {}
                    dict_page_url_value[page_url]["article"][article_key] = article_data_dict[article_key]

                if spec_log:
                    logging.info(f"3.5 - [{len(results_parallelization)}] Artikel wurden verarbeitet.")

                year_key = f"Stern - {year}"
                if year_key not in year_data_stern:
                    year_data_stern[year_key] = {}
                    year_data_stern[year_key]["general_metadata"] = general_metadata

                year_data_stern[year_key][f"{year_key} - {month}"] = dict_page_url_value

                for page_url, page_data_dict in dict_page_url_value.items():
                    if "article" in page_data_dict:
                        try:
                            sorted_articles = dict(sorted(
                                page_data_dict["article"].items(),
                                key=lambda x: list(map(int, re.search(r"(\d+)-(\d+)$", x[0]).groups()))
                            ))
                            page_data_dict["article"] = sorted_articles
                        except Exception as e:
                            logging.warning(f"3.6 - Artikel konnten nicht sortiert werden: {e}")

                with open(year_output_path, "w", encoding="utf-8") as f:
                    json.dump(year_data_stern, f, indent=4, ensure_ascii=False)
                    logging.info(f"3.6 - Datei [{year_output_path}] wurde gespeichert.")

    # Fehlerprotokoll speichern.
    with open(incorrect_data_path, "w", encoding="utf-8") as f:
        json.dump(incorrect_data_stern, f, indent=4, ensure_ascii=False)

    logging.info("3.6 - Grundlegende Verarbeitung abgeschlossen.")
    return output_directory

# Alles unter main aufrufen und durchfuehren.
if __name__ == "__main__":

    ## 0.2 - Logging einrichten.
    
    # Falls bereits Logging existiert, stoppen und flushen (Also alle Loggingaktivitaeten stoppen und offene Log-Dateien schließen.).
    logging.shutdown()
    
    # uebriggebliebene Handler auch loeschen.
    logging.getLogger().handlers.clear()
    
    # Erstellung des Pfades zur Log-Datei.
    log_file = os.path.join(output_folder_local, "process_STERN.log")
    
    # Die Logdatei, deren Name in log_file bestimmt wurde, schreiben, auf DEBUG-Level setzen, 
    # die Struktur auf Zeit - Level - Nachricht setzen und schließlich noch das Datumsformat bestimmen.
    logging.basicConfig(
        filename=log_file,
        filemode="w",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    
    # urllib3 fuers Logging stummschalten.
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    # requests fuers Logging stummschalten.
    logging.getLogger("requests").setLevel(logging.WARNING)

    logging.info("0.1 - Log-Datei wurde eingerichtet.")


    ### 1 - Artikel-URLs extrahieren.

    print(f"1 - Artikel-URLs des Sterns herausfinden. Datum: {str(datetime.now())}")

    # crawl_stern_article_urls(url_stern_archive_default, output_folder_local, spec_log=False)

    logging.info("1 - Artikel-URLs wurden erfolgreich gesammelt.")
    print(f"1 - Artikel-URLs wurden erfolgreich gesammelt. Datum: {str(datetime.now())}")


    ### 2 - HTML-Dateien der Artikel-Websites herunterladen.

    print(f"2 - HTML-Inhalte (Rohdaten) herunterladen. Datum: {str(datetime.now())}")

    # download_articles_by_page_file_stern(output_folder_local, input_file=os.path.join(output_folder_local, "STERN_issues_and_articles_def_1.json"), spec_log=False)

    logging.info("2 - HTML-Inhalte wurden erfolgreich heruntergeladen.")
    print(f"2 - HTML-Inhalte wurden erfolgreich heruntergeladen. Datum: {str(datetime.now())}")

    ### 3 - Daten und Metadaten der Ausgaben und Artikel extrahieren und in JSON-Dateien speichern.

    print(f"3 - Artikeldaten und Metadaten werden nun parallelisiert extrahiert. Datum: {str(datetime.now())}")

    # Hier endlich Daten extrahieren.
    extract_stern_data(input_directory = os.path.join(output_folder_local, "www.stern.de", "stern_archiv"), output_directory = os.path.join(output_folder_local, "stern_json_data_nach_jahren"), output_folder=output_folder_local, spec_log=True)

    # Und schließlich noch einmal die Dateigroeße jeder Datei anfuegen.
    add_file_size_to_output_file_stern(output_directory_input = os.path.join(output_folder_local, "stern_json_data_nach_jahren"), spec_log=False, indexing="3.7")
    logging.info("3.7 - Dateigroessen dem JSON angehaengt.")

    logging.info("4 - Code fuer Stern abgeschlossen.")
    print(f"4 - Code fuer Stern abgeschlossen. Datum: {str(datetime.now())} (Achtung, wegen Multiprocessing bitte trotzdem noch eine Stunde lang den Code nicht anfassen, da diese Nachricht normalerweise schon beim ersten Prozess gedruckt wird.")
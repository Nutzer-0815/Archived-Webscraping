"""
Code zum Download aller Spiegel-Artikel und Ausgaben als Rohdaten (im HTML-Format) [### 1 und ### 2]. 
                                                                --> HTML-Seite des Artikels und HTML-Seite der Ausgabe.
Und dem Verarbeiten und Bereinigen der Spiegel-Artikel und Ausgaben (im JSON-Format) [### 3].

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
    - Datum der letzten Ausfuehrung: 31.05.2025
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
import logging
from difflib import SequenceMatcher
from collections import OrderedDict
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
    
# Die URL des Spiegel-Archivs fuer spaetere Verarbeitung festlegen.
url_spiegel_archive_default = "https://www.spiegel.de/spiegel/print/"

# Sammeldatei erstellen, die alle nicht vorhandenen Webseiten auflistet.
incorrect_data_spiegel = {}

# Pfad erstellen
incorrect_data_path_spiegel = os.path.join(output_folder_local, "spiegel_incorrect_data.json")


### 1 - Die URLs des Spiegels fuer Ausgaben und Artikel herunterladen und in eine JSON-Datei packen.

def crawl_spiegel_archiv_for_issue_and_article_urls(base_url, output_folder, spec_log):
    """
    Holt alle verfuegbaren Archiv-Jahrgangs-URLs, um schließlich alle Ausgabe-URLs und Artikel-URLs zu extrahieren.
    Speichert die Daten in einer JSON-Datei, geordnet nach Jahrgang und Ausgabe-Nummer.
        Parameter: Die URL, mit der es anfaengt, dann der Ordner, in den die finale Datei gespeichert werden soll, und ob gewisse Eckpunkte gedruckt werden sollen.
        Ausgabe: Keine.
    """
    
    ## 1.1 - Finale Dateien erstellen
    
    # Die finale Output-Datei und deren Pfad festlegen (Ich spreche hier von Output, da Ausgabe mit dem Dateninhalt verwechselt werden kann). 
    output_issues_and_articles_file_SPIEGEL = os.path.join(output_folder, "SPIEGEL_issues_and_articles_def_1.json")

    def load_or_initialize_json(file_path, spec_log): #-> Robustheit #-> Effizienz 
        """
        Laedt eine JSON-Datei oder erstellt eine neue leere Liste, falls die Datei nicht existiert oder beschaedigt ist. 
            Parameter 1: file_path, Dateipfad. Gibt aus: leeres JSON-Objekt.
            Parameter 2: spec_log, Bedingung fuer detailliertes Logging.

        """
        if os.path.exists(file_path):
            # Datei auslesen, außer die Datei ist kaputt oder leer, dann laeuft der Code weiter, damit sie ueberschrieben werden kann.
            try:
                with open(file_path, "r", encoding="utf-8") as file:
                    data = json.load(file)
                    if spec_log:
                        logging.info(f"1.2 - Bestehende Datei [{file_path}] ausgelesen.")
                    return data if isinstance(data, dict) else {}
            except (json.JSONDecodeError, IOError):
                logging.error(f"1.2 - Fehler beim Auslesen der bestehenden Datei [{file_path}]: {IOError}")
                return {}
        return {}


    ## 1.2 - Fehlerprotokolle fuer fehlgeschlagene Anfragen einrichten. #-> Wartbarkeit
    
    # Zuerst muessen die Dateipfade und Dateien der Fehlerprotokolle festgelegt werden.
    failed_urls_file = os.path.join(output_folder, "failed_urls_def_1.json")
    failed_issues_file = os.path.join(output_folder, "failed_issues_def_1.json")
    if spec_log:
        logging.info(f"Die Dateiepfade fuer: \n[{failed_urls_file}], \n[{failed_issues_file}], wurden angelegt.")

    # Platz fuer verschachtelte Funktionen, die spaeter abgerufen werden.
    def save_failed_url(url, indexing="1.2"):
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

    def save_failed_issue(issue, indexing="1.2"):
        """
        Speichert eine fehlerhafte Ausgabe-URL in die Fehlerprotokoll-Datei. 
            Parameter: URL der Ausgaben-Website. Indexierung. 
            Ausgabe: Keine.
        """

        if os.path.exists(failed_issues_file):
            # Liest bisherige Fehlerdatei aus (Falls sie Inhalt besitzt, heißt das, es gab einen Absturz, sonst waere sie ueberschieben worden).
            with open(failed_issues_file, "r", encoding="utf-8") as file:
                try:
                    failed_issues = json.load(file)
                except json.JSONDecodeError:
                    failed_issues = {}

            # Neue fehlerhafte Dateien in die Datei schreiben.
            failed_issues[issue] = {"timestamp": datetime.now().isoformat()}
            with open(failed_issues_file, "w", encoding="utf-8") as file:
                json.dump(failed_issues, file, indent=4, ensure_ascii=False)

        # Falls Datei nicht existiert, sie erstellen und Daten einspeisen.
        else:
            failed_issues = {}
            failed_issues[issue] = {"timestamp": datetime.now().isoformat()}
            with open(failed_issues_file, "w", encoding="utf-8") as file:
                json.dump(failed_issues, file, indent=4, ensure_ascii=False)
        logging.warning(f"{indexing} - Fehlerhafte Ausgabe gespeichert: {issue}")

    def save_json_sorted(file_path, data, indexing, spec_log):
        """
        Speichert die JSON-Datei geordnet nach Jahrgang und Ausgabe-Nummer.
            Parameter 1: Dateipfad, wo die Datei gespeichert wird. 
            Parameter 2: Daten, die spaeter als Dictionary erkannt werden.
            Parameter 3: Indexierung.
            Parameter 4: Log-Bedingung.
            Ausgabe: Keine.
        """

        # Hier werden die Schluessel-Wert-Paare unter data aufgerufen, welche nach dem Schluessel per lambda geordnet werden,
        #  dem die HTML-Struktur mit einem r-String zugewiesen werden (Der nach folgendem Muster sucht: "index-0000-00.html"). #-> Robustheit
        sorted_data = dict(sorted(data.items(), key=lambda url: tuple(map(int, re.search(r"index-(\d{4})-(\d+)", url[0]).groups()))))

        # Die Datei schreiben. #-> Robustheit
        try:
            with open(file_path, "w", encoding="utf-8") as file:
                json.dump(sorted_data, file, indent=4, ensure_ascii=False)
            if spec_log:
                logging.info(f"{indexing} - Artikel-URLs wurden geordnet in {file_path} gespeichert.")

        except UnicodeDecodeError as e:
            logging.error(f"{indexing} - UniDecodeError fuer {file_path}: [{e}]")

        except Exception as e:
            logging.error(f"{indexing} - Fehler beim Schreiben der Datei {file_path}: [{e}]")

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
                    save_failed_url(url)
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
            save_failed_url(url)

    # Letzten Fortschritt aus der JSON-Datei laden. #-> Effizienz
    articles_by_issue = load_or_initialize_json(output_issues_and_articles_file_SPIEGEL, spec_log)


    ## 1.3 - Jahrgangs-URLs finden

    # Erst wird ueberprueft, ob die URL ueberhaupt verfuegbar ist, damit der Fehler nicht erst spaeter auftritt. 
    # Im Aufruf der Funktion wird außerdem die Nummer beigegeben, bei der wir uns gerade befinden, was das Debugging deutlich erleichtert. #-> Wartbarkeit
    content_first = fetch_url(base_url, "1.3")
    if not content_first:
        logging.warning(f"1.3 - Die URL [{content_first} wurde nicht gefunden.]")
        return

    # Aus dem HTML-Code ein Soup-Objekt zum weiteren parsen erstellen.
    soup = BeautifulSoup(content_first, "html.parser")

    # Ein leeres set erstellen, in das spaeter alle URLs gepackt werden, um Mehrfachnennungen zu loeschen.
    year_urls = set()

    #!!! allowed_years = {"1998"} # allowed_years = r"\b195[0-9]\b" # 50er  # allowed_years = r"\b(197[0-9]|1980)\b" # 70er und 1980 #!!! debug-Zwecke, spaeter wieder entfernen #-> Wiederverwendbarkeit

    # Sucht nach Jahreszahlen zwischen "1900"-"2009" und schließt dabei aus, dass es zu einer laengeren Zahlenfolge gehoert. 
    # Mit dem Ausschluss aller Zahlen nach 2009 werden die Ausgaben hinter der Paywall ignoriert, die zur Zeit der Codeerstellung (2025) noch existiert.
    pattern_year = r"\b(19[0-9]{2}|200[0-9])\b"

    # Sucht nach bestimmten string, der definitiv bei allen Ausgaben steht.
    archive_links = soup.find_all(string=re.compile("der spiegel archiv", re.IGNORECASE))

    # Geht eine Ebene im HTML-Baum hoeher, da dieser Container die Links enthaelt. Sucht nun nach allen a-tags (die Links markieren), auf die das Muster zutrifft und fuege sie dem Set hinzu.
    for ancestor in archive_links:
        for a_tag in ancestor.find_parents():
            for a in a_tag.find_all("a", href=True):
                href = a["href"]
                if re.search(pattern_year, href) and re.search(pattern_year, a.get_text(strip=True)):
                    year_match = re.search(r"\d{4}", href)
                    if year_match: #!!! and year_match.group() in allowed_years: # Wenn hinzugefuegt wird, einfach nur ": #" loeschen. Wenn weggenommen wird, nach " if year_match:" enden #!!! debug-Zwecke, spaeter wieder entfernen #-> Wiederverwendbarkeit
                        if href not in year_urls: #!!! debug-Zwecke, spaeter wieder hinzufuegen:     #-> Wiederverwendbarkeit
                            year_urls.add(href)


    ## 1.4 - Ausgabe-URLs sammeln
    
    if spec_log:
        logging.info("1.4 - Suche nach Ausgabe-URLs startet.")

    # Laedt die bisher bestehende Datei, um bereits abgefragte Ausgabe-URLs nicht noch einmal abzurufen, so kann an der letzten Stell weitergemacht werden, an der der Code sich aufgehaengt hat.
    issue_urls = set(articles_by_issue.keys())  

    # Speichert die Ausgabe-URLs schon einmal ab. Vermeidet Fehler, falls der Code mal wieder abschmiert. #-> Robustheit
    output_issue_file = os.path.join(output_folder, "spiegel_issue_urls_def_1.txt")

    # Ruft jede einzelne Ausgabe ab, um deren HTML-Content als Response-Objekt zu erhalten, um an neue Ausgabe-URLs zu kommen.
    with open(output_issue_file, "a", encoding="utf-8") as issue_file:
        for year_url in year_urls:
            content = fetch_url(year_url, "1.4")

            # Wenn keine Ausgabe vorhanden ist, einfach weitermachen.
            if not content:
                continue

            # Laedt ein Soup-Objekt aus dem Response-Objekt. 
            soup = BeautifulSoup(content, "html.parser")
            
            # Ueberprueft dann nochmal die URL-Struktur.
            for a in soup.find_all("a", href=True):

                if re.match(r"https://www\.spiegel\.de/spiegel/print/index-\d{4}-\d{1,2}\.html", a["href"]):

                    # Fuegt neue Ausgabe-URLs hinzu.
                    if a["href"] not in issue_urls:  
                        issue_urls.add(a["href"])
                        issue_file.write(a["href"] + "\n")

            if spec_log:
                logging.debug(f"1.4 - Fuer [{year_url}] die Ausgaben-HTML-Seiten erhalten")


    ## 1.5 - Artikel-URLs sammeln und speichern.
    if spec_log:
        logging.info("1.5 - Suche nach Artikel-URLs startet.")

    # Durchlaufe das Dictionary URL fuer URL.
    for issue_url in issue_urls:

        # Hier wird noch einmal ueberprueft, welche Artikel-URLs bereits existieren, damit keine doppelt abgefragt wird.
        if issue_url in articles_by_issue:
            continue  

        # Laedt den HTML-Inhalt der Ausgabe-URL. Speichert bei Fehler die URL.
        content = fetch_url(issue_url, "1.5")
        if not content:
            save_failed_issue(issue_url)
            continue  

        # Laedt den Inhalt als Soup-Objekt, das geparst werden kann.
        soup = BeautifulSoup(content, "html.parser")
        
        # Suche nach Artikeln.
        articles = []
        for a in soup.find_all("a", href=True):

             # Filtert alle Links mit dem Musterstring("context=issue") heraus, der sie als Artikellinks ausweist.
            if "context=issue" in a["href"]:

                # Erst werden alle Links ueberprueft, ob sie relativ sind und fuegt ihnen (falls nicht) eine absolute Struktur an. 
                full_url = f"https://www.spiegel.de{a['href']}" if not a['href'].startswith("http") else a['href']
                # Testen, ob der Artikel abrufbar ist, damit nur funktionierende Artikel-URLs gespeichert werden.
                articles.append(full_url)
                    
        # Falls die Artikel gefunden werden, werden sie als Wert in die finale Datei sortiert gespeichert.
        if articles:
            articles_by_issue[issue_url] = articles  
            save_json_sorted(output_issues_and_articles_file_SPIEGEL, articles_by_issue, indexing="1.5", spec_log=spec_log)
            if spec_log:
                logging.info(f"1.5 - {len(articles)} Artikel fuer {issue_url} gespeichert.")

    if spec_log:
        logging.info(f"1.5 - Gefundene Artikel-URLs gespeichert in {output_issues_and_articles_file_SPIEGEL}")


### 2 - Download der Spiegel-Inhalte

def download_articles_by_issue_file_spiegel(input_file, output_folder, spec_log):
    """
    Laedt Artikel-HTML-Inhalte nach Ausgaben sortiert herunter und speichert sie direkt in einer JSON-Datei (Anhaengen).
        Parameter 1: Pfad zur Datei mit Ausgaben-URLs (JSON-Struktur).
        Parameter 2: Zielordner fuer die Ausgabe.
        Parameter 3: Log-Bedingung.
        Ausgabe: Keine.
    """

    logging.info("2.0 - Download der Rohdaten beginnt nun.")

    # Sicherstellen, dass die Eingabedatei existiert. #-> Robustheit
    if not os.path.exists(input_file):
        logging.error(f"2.0 - Die Datei [{input_file}] wurde nicht gefunden.")
        print(f"Die Datei [{input_file}] wurde nicht gefunden.")
        return

    # URLs aus der Eingabedatei lesen.
    with open(input_file, "r", encoding="utf-8") as file:
        issue_data = json.load(file)

    if spec_log:
        logging.info(f"2.0 - {len(issue_data)} Ausgaben-URLs aus der Datei [{input_file}] geladen.")
    
    # Sicherstellen, dass der output-folder existiert.
    os.makedirs(output_folder, exist_ok=True)
    # print(f"output_folder = {output_folder}") #!!! debug-Zwecke spaeter wieder entfernen.

    # Sicherstellen, dass der Ausgabeordner existiert.
    base_folder = os.path.abspath(os.path.join(output_folder, "www.spiegel.de"))
    os.makedirs(base_folder, exist_ok=True)

    # Fehlerprotokolle. #-> Wartbarkeit
    failed_issues_file = os.path.join(output_folder, "failed_issues_spiegel_def_2.json")
    failed_articles_file = os.path.join(output_folder, "failed_articles_spiegel_def_2.json")
    
    # Fehlerdaten initialisieren.
    failed_issues = {}
    failed_articles = {}

    # Pruefen, ob Fehlerprotokolle existieren und laden. #-> Wartbarkeit
    if os.path.exists(failed_issues_file):
        with open(failed_issues_file, "r", encoding="utf-8") as file:
            failed_issues = json.load(file)
    if os.path.exists(failed_articles_file):
        with open(failed_articles_file, "r", encoding="utf-8") as file:
            failed_articles = json.load(file)

    def generate_issue_folder_name(url, indexing): #-> Wartbarkeit
        """
        Menschenlesbaren Ordnernamen aus der Ausgabe-URL extrahieren.
        Beispiel: https://www.spiegel.de/spiegel/print/index-1947-46.html -> spiegel_1947_46
            Parameter: Die URL. Die Indexierung.
            Ausgabe: Der Dateiname der Ausgabe.
        """
        try: 
            parts = url.split("/")[-1].replace("index-", "").replace(".html", "").split("-")
            if spec_log:
                logging.info(f"{indexing} - lesbarer Ordner fuer spiegel_{parts[0]}_{parts[1]} erstellt.")
            return f"spiegel_{parts[0]}_{parts[1]}"
        except Exception as e:
            logging.error(f"{indexing} - Fehler beim Generieren des Ordnernamens aus URL [{url}]: {e}")
            return "unknown_issue"

    def handle_rate_limit(pause_durations, attempt, indexing): #-> Robustheit
        """
        Verarbeite die Rate-Limit-Pausen-Logik basierend auf der aktuellen Anzahl der Versuche.
            Parameter 1: Die Minutenanzahl, die gewartet werden muss.
            Parameter 2: Die Anzahl der Versuche.
            Parameter 3: Indexierung.
            Ausgabe: Keine.
        """

        pause_time = pause_durations[min(attempt, len(pause_durations) -1)]
        logging.warning(f"{indexing} - Rate-Limiting erkannt. Warte {pause_time} Minuten.")
        time.sleep(pause_time * 60)  # Pause in Minuten.


    ## 2.1 - Session fuer Anfragen erstellen. #-> Effizienz

    with requests.Session() as session:
        for count, (issue_url, article_urls) in enumerate(issue_data.items()):
            
            #!!! Debug-Zwecke: Falls man nur ein paar Ausgaben testweise verarbeiten moechte, muss folgender Code aktiviert werden: #-> Wiederverwendbarkeit
            #!!! max_count = 25 #!!! Debug-Zwecke
            #!!! if count >= max_count: #!!! Debug-Zwecke
            #!!!     logging.info("2.1 - Die maximale Anzahl von [{max_count}] Ausgaben wurde erreicht. Abbruch. (Debug-Zwecke)") #!!! Debug-Zwecke
            #!!!     print("2.1 - Die maximale Anzahl von 4 Ausgaben wurde erreicht. Abbruch. (Debug-Zwecke)") #!!! Debug-Zwecke
            #!!!     break #!!! Debug-Zwecke
            
            if spec_log:
                logging.info(f"Beginne Verarbeitung von Ausgabe: [{issue_url}].")

            # Ausgabe-Ordner erstellen.
            issue_folder_name = generate_issue_folder_name(issue_url, "2.1")
            issue_folder = os.path.join(base_folder, issue_folder_name)
            os.makedirs(issue_folder, exist_ok=True)


            ## 2.1.1 - Ausgabe-HTML speichern.

            issue_html_content = os.path.join(issue_folder, "index.html")
            pause_durations = [1, 2, 3, 4, 5, 10, 20, 30, 60, 120] #-> Robustheit #-> Effizienz

            if not os.path.exists(issue_html_content): #-> Effizienz
                attempt = 0
                while attempt < len(pause_durations):
                    try:
                        if spec_log:
                            logging.info(f"2.1.1 - Anfrage fuer [{issue_url}].")

                        # Wartezeit einfuegen.
                        delay = random.uniform(0.2, 0.8) 
                        time.sleep(delay)
                        if spec_log:
                            logging.debug(f"Wartezeit vor Anfrage: {delay:.2f} Sekunden")

                        response = session.get(issue_url, timeout=30) # Hier den Status-Code auf 200 setzen, im Gegensatz zu ## 1.3, ## 1.4 und ## 1.5, da es hier von Relevanz sein koennte. # Timeout auf 30 Sekunden gesetzt.
                        
                        if response.status_code == 200:
                            # Wenn Seite gefunden wurde, dir URL als Dateinamen nutzen und das ganze Response-Objekt als Text in HTML-Datei abspeichern.
                            with open(issue_html_content, "w", encoding="utf-8") as file:
                                file.write(response.text)
                            if spec_log:
                                logging.info(f"2.1.1 - Ausgabe [{issue_url}] HTML gespeichert.")
                            break
                        else:
                            logging.warning(f"2.1.1 - Fehlerhafte Ausgabe-URL: [{issue_url}] mit Status [{response.status_code}]")
                            failed_issues[issue_url] = {
                                "timestamp": datetime.now().isoformat(),
                                "status_code": response.status_code
                            }
                            break

                    except requests.exceptions.Timeout: #-> Wartbarkeit
                        logging.error(f"2.1.1 - Timeout bei der Anfrage an [{issue_url}]. Versuch {attempt + 1}.")
                        handle_rate_limit(pause_durations, attempt, "2.1.1")
                        attempt += 1

                    except Exception as e: #-> Wartbarkeit
                        logging.error(f"2.1.1 - Fehler beim Abrufen der Ausgabe [{issue_url}]: {e}")
                        failed_issues[issue_url] = {
                            "timestamp": datetime.now().isoformat(),
                            "error": str(e)
                        }
                        break

                else:
                    logging.error(f"2.1.1 - Abbruch nach mehreren fehlgeschlagenen Versuchen fuer Ausgabe [{issue_url}].")
                    failed_issues[issue_url] = {
                        "timestamp": datetime.now().isoformat(),
                        "error": "Mehrere fehlgeschlagene Timeout-Versuche"
                    }
                    continue
            else:
                if spec_log:
                    logging.debug(f"2.1.1 - Ausgabe [{issue_url}] existiert bereits. Ueberspringe.")
                pass


            ## 2.1.2 Artikel speichern.

            for article_url in set(article_urls):

                # Da Windows nur so circa 260 Zeichen lange Dateinamen nutzt (inklusive Pfad?) und dabei aber die Dateiendungen mit dazu zaehlen, 
                # wird hier einfach auf 5 weniger als Maximallaenge runtergekuerzt. Die URL als Metadatum wird spaeter soweiso aus dem HTML gezogen und nicht
                # aus dem Dateinamen.
                max_path_length = 255
                suffix = ".html"
                base_name = article_url.split("/")[-1].split("?")[0]
                max_name_length = max_path_length - len(suffix) - len(issue_folder) - 5 # For good measure, also Backslashs und so.
                article_html_file = base_name[:max_name_length] + suffix
                article_path = os.path.join(issue_folder, article_html_file)

                if os.path.exists(article_path): # Ueberschreibschutz.
                    logging.debug(f"2.1.2 - Artikel [{article_url}] existiert bereits. Ueberspringe.")
                    continue

                attempt = 0
                # Solange man nicht laenger als die Pausen Anfragen schickt, weitermachen.
                while attempt < len(pause_durations):
                    try:
                        if spec_log:
                            logging.info(f"2.1.2 - Rufe Artikel ab: [{article_url}]")
                        response = session.get(article_url, timeout=30)
                        if response.status_code == 200:
                            # Wenn Seite gefunden wurde, dir URL als Dateinamen nutzen und das ganze Response-Objekt als Text in HTML-Datei abspeichern.
                            with open(article_path, "w", encoding="utf-8") as file:
                                file.write(response.text)
                            if spec_log:
                                logging.info(f"2.1.2 - Artikel [{article_url}] HTML gespeichert.")
                            break
                        else:
                            logging.warning(f"2.1.2 - Fehlerhafte Artikel-URL: [{article_url}] mit Status [{response.status_code}]")
                            failed_articles[article_url] = {
                                "timestamp": datetime.now().isoformat(),
                                "status_code": response.status_code
                            }
                            break

                    except requests.exceptions.Timeout: #-> Wartbarkeit
                        logging.error(f"2.1.2 - Timeout bei der Anfrage an [{article_url}]. Versuch {attempt + 1}.")
                        handle_rate_limit(pause_durations, attempt, "2.1.2")
                        attempt += 1

                    except Exception as e: #-> Wartbarkeit
                        logging.error(f"2.1.2 - Fehler beim Abrufen des Artikels [{article_url}]: {e}")
                        failed_articles[article_url] = {
                            "timestamp": datetime.now().isoformat(),
                            "error": str(e)
                        }
                        break

                else:
                    logging.error(f"2.1.2 - Abbruch nach mehreren fehlgeschlagenen Versuchen fuer Artikel [{article_url}].")
                    failed_articles[article_url] = {
                        "timestamp": datetime.now().isoformat(),
                        "error": "Mehrere fehlgeschlagene Timeout-Versuche"
                    }

    # Fehlerprotokolle speichern. #-> Wartbarkeit
    with open(failed_issues_file, "w", encoding="utf-8") as file:
        json.dump(failed_issues, file, indent=4, ensure_ascii=False)

    with open(failed_articles_file, "w", encoding="utf-8") as file:
        json.dump(failed_articles, file, indent=4, ensure_ascii=False)

    logging.info(f"2 - Verarbeitung abgeschlossen. {len(issue_data)} Ausgaben verarbeitet.")


### 3 - Daten und Metadaten der Ausgaben und Artikel extrahieren und in JSON-Dateien speichern.

## 3.0.1 - Verschiedene Funktionen fuer die Verarbeitung vordefinieren.

def is_html_empty(file_path, indexing): #-> Wartbarkeit
    """
    Funktion ueberprueft, ob die Datei leer ist, bzw. ob der HTML-Body leer ist.
        Parameter 1: Dateipfad der einzelnen Websites.
        Parameter 2: Indexierung zur Nummerierung der Nachrichten andererseits.
        Ausgabe: None, leerer String oder boolsches True
    """
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            soup = BeautifulSoup(file, "html.parser")
            # Checken ob es ueberhaupt einen body gibt.
            body = soup.find("body")
            return body is None or body.text.strip() == ""
    except Exception as e:
        logging.error(f"{indexing} - Es gab ein Problem mit dem HTML-Inhalt: {e}")
        return True


def clean_author_name_spiegel(name):
    """Entferne unerwuenschte Zeichen wie Kommas aus dem Namen"""
    return re.sub(r"[,\s]+", " ", name).strip()


def extract_issue_metadata_spiegel(issue_path_input, issue_number_input, indexing):
    """
    Extrahiert Metadaten einer Ausgabe aus der index.html-Datei.
    
    Eingabewerte (Parameter) fuer die Funktion sind:
        issue_path_input, ist der Pfad der Eingabedatei, mit der die Extraktion beginnt.
        issue_number_input, was hauptsaechlich die Log-Nachrichten als Kennzeichen beherbergen, allerdings ist auch die Verwendung als JSON-Schluessel der Enddatei dabei.
        indexing, gibt mehr Nachrichten dem Log bei.
    Gibt folgende Rueckgabedaten in Tupelform zurueck:
        issue_value, die Ausgabe-Metadaten, an die spaeter die Artikel-Metadaten angefuegt werden.
        issue_date, das wird spaeter bei den Artikeln zur ueberpruefung des Urheberrechts gezueckt.
        reading_time_by_url, werden die Artikel als Metadatum erben.
    """
    logging.debug(f"{indexing} - Extrahiere Metadaten fuer [{issue_number_input}] aus [{issue_path_input}]")
    try:
        if not os.path.exists(issue_path_input):
            logging.warning(f"{indexing} - Keine index.html fuer [{issue_number_input}] gefunden. Ueberspringe.")
            return None, None, None

        # Gewisse Metadaten aus dem Soup-Objekt auslesen, das wiederum aus der .html-Datei ausgelesen wurde.
        with open(issue_path_input, "r", encoding="utf-8") as f:

            # eher ein Sicherheitsschritt, der aus Debugginggruenden dringeblieben ist.
            html_content = f.read()
            soup = BeautifulSoup(html_content, "html.parser")

            title_section = soup.select_one("main#Inhalt section")

            # None, da JSON daraus automatisch Null macht: immer so nah wie moeglich an der Formattierung der Enddatei bleiben.
            issue_title = title_section["aria-label"] if title_section else None

            # Schritt 1: <img> finden mit passendem title-Attribut.
            img_tag = soup.find("img", title=issue_title)

            if img_tag:
                # Schritt 2: naechstes <h2> nach <img> finden, das denselben Text hat.
                next_h2 = img_tag.find_next("h2", string=issue_title)

                if next_h2:
                    # Schritt 3: naechstes <p>-Tag nach diesem <h2>.
                    next_p = next_h2.find_next("p")
                    if next_p:
                        issue_subtitle = next_p.get_text(strip=True)
                    else:
                        issue_subtitle = None
                else:
                    issue_subtitle = None
            else:
                issue_subtitle = None

            canonical_link = soup.find("link", rel="canonical")
            issue_url = canonical_link["href"] if canonical_link else None

            # Datum heraussuchen.
            date_span = soup.find_all("span", class_="relative bottom-px")
            issue_date = None
            for span in date_span:
                issue_date_match = re.search(r"\d{2}\.\d{2}\.\d{4}", span.text)
                if issue_date_match:
                    issue_date = datetime.strptime(issue_date_match.group(), "%d.%m.%Y").date()
                    break

            # Falls die wichtigsten Daten fehlen, es in die Fehldatei uebertragen.
            if not all([issue_title, issue_url, issue_date]):
                incorrect_data_spiegel.setdefault(issue_number_input, {})["metadata - issue"] = f"Fehlende/ Fehlerhafte Daten (bezueglich Ausgaben) gefunden. URL: [{issue_url}], Date: [{issue_date}]"
                logging.warning(f"{indexing} - Fehlende, oder fehlerhafte Metadaten in Ausgabe [{issue_number_input}]. URL: [{issue_url}], Date: [{issue_date}]")

            # Fuer Artikel-Metadaten schonmal die reading-time aus der Ausgaben-HTML-Seite herausziehen.
            # Suche alle Artikel mit aria-label.
            reading_time_by_url = {}

            for article_tag in soup.find_all("article"):
                # URL extrahieren.
                a_tag = article_tag.find("a", href=True)
                if not a_tag:
                    continue

                article_url = a_tag["href"]
                full_url = f"https://www.spiegel.de{article_url}" if not article_url.startswith("http") else article_url
                # Entferne genau "?context=issue" aus der URL, wenn es vorhanden ist, damit es spaeter matcht.
                full_url = full_url.replace("?context=issue", "")
                # Lesedauer extrahieren.
                match = re.search(r"\d{1,2}\s?Min", article_tag.get_text())
                if match:
                    reading_time_by_url[full_url] = match.group(0)

            # Einer Variable alle Metadaten zuweisen.
            issue_value = {
                "issue_number": issue_number_input,
                "issue_title": issue_title,
                "issue_subtitle": issue_subtitle,
                "issue_url": issue_url,
                "issue_publication_date": issue_date.isoformat() if issue_date else None,
                "article": {}
            }

            # Die Variablen muessen natuerlich auch zurueckgegeben werden, duh.
            return issue_value, issue_date, reading_time_by_url
        
        if html_content and not html_content.strip():
            incorrect_data_spiegel.setdefault(issue_number_input, {})["metadata - issue"] = f"Leere Ausgaben-HTML fuer [{issue_number_input}] gefunden."
            logging.error(f"{indexing} - Leere Datei fuer [{issue_number_input}]")
            return None, None, None

    except Exception as e:
        logging.error(f"{indexing} - Fehler beim Verarbeiten der Ausgabe [{issue_number_input}]: {e}")
        return None


def extract_article_metadata_spiegel(issue_number_input, reading_time_by_url_input, issue_folder_input, issue_date_input, article_file_input, spec_log=False, indexing="3.5"):
    """
    Alle Daten und Metadaten des Artikels holen.
    Man koennte jeder einzelnen Metadatenextraktion eine eigene Funktion zuweisen, 
    doch wuerde sich das als nicht so uebersichtlich erweisen.
    Um die Logik besser zu verstehen, wird hier ein kontinuierlicher Stream empfohlen.
    
    Die Eingabewerte (Parameter/ Argumente) sind mit folgenden Werten versehen:
        issue_number_input, dient der richtigen Zuweisung von Log-Nachrichten, Anheftung der Artikel und dem Fehlerprotokoll.
        reading_time_by_url_input, unveraendert in das gleichnamige Metadatum einlegen.
        issue_folder_input, was den richtigen Pfad fuer die Artikel-HTML-Dateien definieren soll.
        issue_date_input, wird der Urheberrechtspruefung uebergeben.
        article_file_input, das ist die input-HTML-Datei, hier muessen wir die außerhalb erfolgenden Iterationen ueber article_files festlegen.
        spec_log, Bedingung, die ausfuehrliches Logging hinzufuegt.
        indexing, Nummerierung fuer Log-Nachrichten, damit die Reihenfolge nicht truegt.
    Vier Ausgabewerte werden zurueckgegeben:
        article_data_dict, alle Metadaten des Artikels, welche man spaeter an die Ausgabemetadaten anfuegt, wie der Name schon sagt, als Dictionary eben.
        issue_number_input, damit bei der Parallelisierung der Coder weiß, welcher Artikel zu welcher Ausgabe gehoert, daher wird er unveraendert ausgegeben, was genuegt.
        article_key, der Schluessel der die Artikel in der finalen Datei ausweist.
        incorrect_data_spiegel, dort versammeln sich alle fehlerhaften Daten in einem Kreis.
    """

    # Die zurueckgebenden Werte einen default geben, damit es spaeter nicht zu einem Verarbeitungsfehler kommt, 
    # sollte der Artikel leer sein und der Code versuchen, auf seine return-Werte zuzugreifen.
    article_data_dict = {}
    incorrect_data_spiegel = {}
    article_number = None
    article_key = "article - Null"

    # Gesamten Pfad fuer einzelne Dateien erstellen.
    article_path = os.path.join(issue_folder_input, article_file_input)
    
    # ueberprueft, ob Artikeldatein leer sind und speichert sie in Fehlerdatei.
    if is_html_empty(article_path, indexing):
        incorrect_data_spiegel.setdefault(issue_number_input, {})["Article"] = "Leerer Artikel gefunden."
        logging.warning(f"{indexing} - Leerer Artikel in Ausgabe [{issue_number_input}] gefunden.")
        return {}, issue_number_input, article_key
    
    try:
        # Auf die html-Datei zugreifen.
        with open(article_path, "r", encoding="utf-8") as f:
            article_soup = BeautifulSoup(f, "html.parser")

        # Titel finden.
        title_tag = article_soup.find("title")
        article_title = re.sub(r" - DER SPIEGEL", "", title_tag.get_text(strip=True), flags=re.IGNORECASE) if title_tag else None

        # URL finden.
        url_meta = article_soup.find("meta", attrs={"property": "og:url"})
        article_url = url_meta["content"].strip() if url_meta else None

        # Veroeffentlichungsdatum des Artikels extrahieren.
        article_publication_date = None
        meta_tag = article_soup.find("meta", attrs={"name": "last-modified"})
        if meta_tag and meta_tag.has_attr("content"):
            try:
                article_publication_date = datetime.fromisoformat(meta_tag["content"])
            except ValueError:
                if spec_log:
                    logging.warning(f"{indexing} - Ungueltiges Datumsformat in last-modified-Tag: {meta_tag['content']}")

        # Den Anchor-tag "Zur Ausgabe" finden, bei dem alle herauszuloeschenden p-tags sind, die nicht in den Volltext gehoeren.
        zur_ausgabe_link = article_soup.find("a", title=re.compile(r"\s*Zur\s*Ausgabe\s*", re.IGNORECASE), string=re.compile(r"\s*Zur\s*Ausgabe\s*", re.IGNORECASE))
        
        # Fallback: Falls das nicht auffindbar ist, einfach alle a-tags mit Ausgabe finden.
        # Das Eltern-tag finden, in dem auch die Ausnahmen enthalten sind.
        if zur_ausgabe_link is not None:
            parent_div = zur_ausgabe_link.parent
            
            # Falls das Eltern-tag nicht direkt div ist, eben das noechste finden.
            if parent_div.name != "div":
                parent_div = zur_ausgabe_link.find_parent("div")

            # Alle elendigen p-tags finden.
            p_tags = parent_div.find_all("p")

            # Text jedes einzelnen p-tags extrahieren.
            exception_texts = [p.get_text(strip=True) for p in p_tags]
        
        else: 
            logging.warning(f"{indexing} - 'Zur Ausgabe'-Link in Artikel [{article_file_input}] nicht gefunden.")
            incorrect_data_spiegel.setdefault(issue_number_input, {}).setdefault("article_data_dict", {})[article_file_input] = "Kein 'Zur Ausgabe'-Link gefunden."
            exception_texts = []

        # Text finden.
        paragraphs_texts = article_soup.find_all("p")
        # Nun die nicht zum Volltext gehoerenden Texte herausnehmen.
        filtered_paragraphs = [p for p in paragraphs_texts if p.get_text(strip=True) not in exception_texts]
        # Extra Variablen zum Auszaehlen mit Soup-Objekten erstellen.
        filtered_paragraphs_soup = [p for p in paragraphs_texts if p.get_text(strip=True) and p.get_text(strip=True) not in exception_texts]
        # Die HTML-Elemente loeschen, um den Text gescheit zaehlen zu koennen.
        # Der Wordcount trennt bisher Bindestriche. Und Zahlen die so geschrieben werden: 500 000 oder 500.000 (statt eigentlich 500000) werden auch getrennt gezaehlt.
        clean_text = " ".join(p.get_text(strip=True) for p in filtered_paragraphs_soup) if filtered_paragraphs_soup else ""

        # Die tatsaechlichen Zahlen auszaehlen.
        if clean_text:
            word_count = len(clean_text.split())
            char_count = len(clean_text)
        else:
            word_count = 0
            char_count = 0

        # Nun es wirklich zu einem zusammenpacken (aber die HTMl-Tags drinnen lassen. 
        # Auf diese Weise koennte jemand, der spaeter durch den text geht, per bs4 .get_text einfach die tags entfernen.).
        article_text = "".join([str(p) for p in filtered_paragraphs]) if filtered_paragraphs else None
        
        # Extrahiere den Artikeluntertitel.
        meta_description = article_soup.find("meta", attrs={"name": "description"})
        article_subtitle = meta_description["content"].strip() if meta_description else None
        # Entferne das Zeichen "…" am Ende des Untertitels, falls vorhanden.
        article_subtitle = article_subtitle.rstrip("…").strip() if article_subtitle else None

        # Wenn der Untertitel im Text vorhanden ist, ist es nicht wirklich ein Untertitel, sondern eine Textvorschau.
        # In diesem Fall muss der Untertitel frei bleiben.
        if article_subtitle and article_text:
            subtitle_clean = article_subtitle.strip()
            title_clean = article_title.strip() if article_title else ""

            # Typografische Varianten vereinheitlichen (nur Satzzeichen, keine Buchstaben).
            substitutions = {
                "«": "\"", "»": "\"", "„": "\"", "“": "\"", "‚": "'", "‘": "'", "’": "'",
                "–": "-", "—": "-", "‐": "-", "‑": "-", "…": "...",
            }

            # Die entsprechenden Zeichen bei beiden ersetzen.
            for key, val in substitutions.items():
                subtitle_clean = subtitle_clean.replace(key, val)
                title_clean = title_clean.replace(key, val)

            # Doppelte Leerzeichen durch einfache ersetzen.
            subtitle_clean = re.sub(r"\s+", " ", subtitle_clean).strip()
            title_clean = re.sub(r"\s+", " ", title_clean).strip()

            # Endlich Vergleich auf Identitaet (nach typografischer Normalisierung).
            # Hier wird Titel und Untertitel verglichen.
            # Warum das indentiert ist weiß ich auch nicht so recht (eigentlich muesste es das nicht), es funktioniert so auf jeden Fall besser.
            if subtitle_clean.lower() == title_clean.lower():
                article_subtitle = None

            else:
                # Alternative Entfernung, wenn Untertitel im Anfang des Artikels oder sehr aehnlich ist.
                subtitle_cmp = re.sub(r"[^\waeoeueaeoeueß .,]", "", subtitle_clean.lower())
                text_plain = BeautifulSoup(article_text, "html.parser").get_text(separator=" ").lower()
                text_plain = re.sub(r"\s+", " ", text_plain)
                text_cmp = re.sub(r"[^\waeoeueaeoeueß .,]", "", text_plain[:len(subtitle_cmp)+200])

                # Ebenfalls danach schauen, ob die aehnlichkeit einen gewissen Grad hat, dadurch greift die Bedingung auch in manchen Sonderfaellen.
                similarity_ratio = SequenceMatcher(None, subtitle_cmp, text_cmp).ratio()
                if subtitle_cmp in text_cmp or similarity_ratio > 0.85:
                    article_subtitle = None

        if spec_log:
            logging.debug(f"{indexing} - Untertitel [{article_subtitle}] wurde extrahiert.")
        
        # Extrahiere Artikelueberschrift (Unterschied zu Titel). Verdammte Typographie, treibt mich noch in den Wahnsinn. Andere Namen: Kicker, Tagline, Dachzeile, 
        # Suche nach dem tag <script type="application/ld+json">. 
        # Hier kann nach ld+json direkt gesucht werden, da dass die Standardformatierung in HTML fuer JSON-LD ist und sich dieser type wohl nicht so schnell aendern wird.
        ld_json_script = article_soup.find("script", type="application/ld+json")
        article_kicker = None

        if ld_json_script:
            try:
                # Mehrere JSON-Objekte moeglich, darum zuerst laden: Inhalt des script-Tags ist reines JSON.
                ld_json_data = json.loads(ld_json_script.string.strip())
                kicker = None

                # Falls es sich um eine Liste handelt, suche das Element mit "@type": "NewsArticle".
                if isinstance(ld_json_data, list):
                    for entry in ld_json_data:
                        if isinstance(entry, dict) and entry.get("@type") == "NewsArticle":
                            raw_kicker = entry.get("headline")
                            kicker = raw_kicker.strip() if isinstance(raw_kicker, str) else None
                            break
                elif isinstance(ld_json_data, dict) and ld_json_data.get("@type") == "NewsArticle":
                    raw_kicker = ld_json_data.get("headline")
                    kicker = raw_kicker.strip() if isinstance(raw_kicker, str) else None
                else:
                    kicker = None

                # Setze die Variable nur, wenn sie nicht mit dem Title uebereinstimmt und nicht leer ist.
                if kicker and kicker != article_title:
                    article_kicker = kicker
                else:
                    article_kicker = None

            except json.JSONDecodeError as e:
                logging.warning(f"{indexing} - ld+json konnte nicht geparsed werden: {e}")

        # Artikelnummer extrahieren, alles mit fuehrenden Nullen auffuellen (Fuer Effizienz, das sollte Rechenleistung beim Sortieren deutlich sparen).
        article_number_match = re.search(r"Artikel\s*([0-9]*)\s*\/\s*([0-9]*)", article_soup.get_text())

        if article_number_match:
            num1 = article_number_match.group(1).zfill(3)
            num2 = article_number_match.group(2).zfill(3)
            article_number = f"{num1}-{num2}"
        else:
            article_number = None
        if spec_log:
            logging.debug(f"{indexing} - Artikelnummer [{article_number}] wurde extrahiert.")

        # Autoren und Rubrik extrahieren.
        author_script_tag = article_soup.find("script", type="application/ld+json")
        authors = []
        category_section = []

        if author_script_tag:

            try:
                json_data_in_html = json.loads(author_script_tag.string)

                if isinstance(json_data_in_html, list):
                    for entry in json_data_in_html:

                        if isinstance(entry, dict) and entry.get("@type") == "NewsArticle":

                            # Rubrik extrahieren.
                            if "articleSection" in entry:
                                category_section.extend(entry["articleSection"] if isinstance(entry["articleSection"], list) else [entry["articleSection"]])

                            # Autoren extrahieren.
                            if "author" in entry:
                                for author in entry["author"]:
                                    if isinstance(author, dict) and author.get("@type") == "Person":
                                        cleaned_name = clean_author_name_spiegel(author["name"])
                                        if cleaned_name:
                                            authors.append(cleaned_name)

                elif isinstance(json_data_in_html, dict):
                    if json_data_in_html.get("@type") == "NewsArticle":

                        # Rubrik extrahieren.
                        if "articleSection" in json_data_in_html:
                            category_section.extend(json_data_in_html["articleSection"] if isinstance(json_data_in_html["articleSection"], list) else [json_data_in_html["articleSection"]])

                        # Autoren extrahieren.
                        for author in json_data_in_html.get("author", []) if isinstance(json_data_in_html.get("author"), list) else []:
                            for author in json_data_in_html["author"]:

                                if isinstance(author, dict) and author.get("@type") == "Person":
                                    cleaned_name = clean_author_name_spiegel(author["name"])

                                    if cleaned_name:
                                        authors.append(cleaned_name)

            except (json.JSONDecodeError, TypeError, ValueError) as e:
                logging.error(f"{indexing} - JSON-Fehler in {article_file_input}: {e}")

        article_authors = authors if authors else None
        article_category = category_section if category_section else None

        if spec_log:
            logging.debug(f"{indexing} - Autoren [{article_authors}] und Rubriken [{article_category}] wurden extrahiert.")

        # Schlagwoerter extrahieren.
        keywords_meta = article_soup.find("meta", attrs={"name": "news_keywords"})

        if keywords_meta:
            keywords = [keyword.strip() for keyword in keywords_meta["content"].split(",")]
        else:
            keywords = None
        if spec_log:
            logging.debug(f"{indexing} - Schlagwoerter [{keywords}] wurden extrahiert.")

        # Lesedauer aus den schon unter extract_issue_metadata herausgezogenen Daten extrahieren.
        reading_time = None
        reading_time_bool = False

        if article_url in reading_time_by_url_input:
            reading_time = reading_time_by_url_input[article_url]
            if reading_time:
                reading_time_bool = True

            if spec_log:
                logging.debug(f"{indexing} - Lesedauer '{reading_time}' fuer Artikel [{article_url}] gefunden und als Artikel-Metadatum zwischengespeichert.")

        if spec_log:
            logging.debug(f"{indexing} - Lesedauer-Metadaten wurden extrahiert.")

        # Urheberrecht  - Berechne das heutige Datum und das Datum vor 70 Jahren.
        date_today = datetime.today()
        date_70_years_ago = date_today - timedelta(days=70*365.25)  # Beruecksichtigt Schaltjahre.
        copyright_bool = None

        # Erst muss die Existenz geprueft werden, da None nicht von strptime verarbeitet werden kann und dies sonst zu einem Fehler fuehren wuerde.
        if issue_date_input is not None: 
            publication_date = datetime.strptime(issue_date_input.isoformat(), "%Y-%m-%d")
            copyright_bool = publication_date > date_70_years_ago
        else:
            copyright_bool = None
        if spec_log:
            logging.debug(f"{indexing} - Urheberrechtsgueltigkeit wurde extrahiert.")

        # Kommentarfunktion extrahieren.
        comments_bool = None
        settings_script_tag = article_soup.find("script", type="application/settings+json")#

        if settings_script_tag:
            try:
                json_data_in_html = json.loads(settings_script_tag.string)
                if "isCommentsEnabled" in json_data_in_html:
                    comments_bool = json_data_in_html["isCommentsEnabled"]
            except (json.JSONDecodeError, TypeError, ValueError) as e:
                logging.error(f"{indexing} - JSON-Fehler beim Extrahieren der Kommentarfunktion in {article_file_input}: {e}")

        # Default-Wert auf False setzen, falls keine Information vorhanden.
        if comments_bool is None:
            comments_bool = False

        if spec_log:
            logging.debug(f"{indexing} - Kommentarfunktion wurde extrahiert.")

        # Merken-Button pruefen.
        bookmark_button_bool = False
        button_tags = article_soup.find_all("button")

        for button in button_tags:
            if "bookmarkbutton" in [cls.lower() for cls in button.get("class", [])] or "bookmarkbutton" in button.get("data-component", "").lower():
                bookmark_button_bool = True
                break

        # Suche nach "data-bookmark-button-el" Attributen als Fallback.
        if not bookmark_button_bool:
            bookmark_elements = article_soup.find_all(attrs={"data-bookmark-button-el": True})
            if bookmark_elements:
                bookmark_button_bool = True

        # Link-Teilen ueberpruefen.
        copylink_button_bool = False

        for button in button_tags:
            if "copylink" in [cls.lower() for cls in button.get("class", [])] or "copylink" in button.get("x-ref", "").lower():
                copylink_button_bool = True
                break

        # Suche nach Attributen als Fallback.
        if not copylink_button_bool:
            copylink_elements = article_soup.find_all(attrs={"data-sara-cta": re.compile(r"^sharing:\s*Link\s*kopieren$", re.IGNORECASE)})
            if copylink_elements:
                copylink_button_bool = True

        # E-Mail ueberpruefen.
        email_link_bool = False
        email_link = article_soup.find("a", attrs={"data-sara-cta": re.compile(r"^sharing:\s*(E\s*-?\s*Mail|Email)$", re.IGNORECASE)})

        if email_link:
            email_link_bool = True
        if spec_log:
            logging.debug(f"{indexing} - Buttons wurden extrahiert.")

        # Plattformen finden.
        platform_list = []

        # Rawstrings der Plattformen festlegen, wie sie eventuell innerhalb des HTMLs stehen koennten.
        # keine Neonazi-Sprache uebernehmen, daher nicht X.com anerkennen.
        hyperlinks_platforms = {
            "Facebook": r"^sharing:\s*Facebook(?:\.(com|de|net))?$",
            "Twitter": r"^sharing:\s*Twitter(?:\.(com|de|net))?$",
            "Twitter": r"^sharing:\s*X.com$",
            "TikTok": r"^sharing:\s*Tik\s*Tok(?:\.(com|de|net))?$",
            "Instagram": r"^sharing:\s*Instagram(?:\.(com|de|net))?$",
            "Reddit": r"^sharing:\s*Reddit(?:\.(com|de|net))?$",
            "Youtube": r"^sharing:\s*You\s*tube(?:\.(com|de|net))?$",
            "Pinterest": r"^sharing:\s*Pinterest(?:\.(com|de|net))?$",
            "Tumblr": r"^sharing:\s*Tumblr(?:\.(com|de|net))?$",
            "LinkdIn": r"^sharing:\s*Linkd\s*In(?:\.(com|de|net))?$"
        }

        for target_platform, url in hyperlinks_platforms.items():
            if article_soup.find("a", attrs= {"data-sara-cta": re.compile(url, re.IGNORECASE)}):
                platform_list.append(target_platform)

        if spec_log:
            logging.debug(f"{indexing} - Plattformen gefunden: {platform_list}")

        # Letztes Update-Datum suchen.
        last_updated_date = None
        # Suche das Script-Tag mit `minUpdatedAt`.
        script_tag = article_soup.find("script", string=re.compile(r'"minUpdatedAt"\s*:\s*\d+', re.IGNORECASE))

        if script_tag:
            try:
                # Lade den JSON-Inhalt.
                json_data = json.loads(script_tag.text)

                # Greife direkt auf den `minUpdatedAt`-Wert zu.
                min_updated_at = json_data.get("general", {}).get("consent", {}).get("minUpdatedAt")

                if min_updated_at:
                    last_updated_date = datetime.utcfromtimestamp(int(min_updated_at)).isoformat()

            except Exception as e:
                logging.error(f"{indexing} - Fehler beim JSON-Parsen von minUpdatedAt: {e}")

        if spec_log:
            logging.debug(f"{indexing} - Letztes Update-Datum: [{last_updated_date}]")

        # Den Schluessel des spaeteren JSON-Elements festlegen. 
        # Falls keine Nummer gefunden wird, wird None uebergeben und es in da Fehlerprotokoll gespeichert.
        if article_number:
            article_key = f"article - {article_number}"

        else:
            article_key = "article - null"
            if os.path.exists(incorrect_data_path_spiegel):

                with open(incorrect_data_path_spiegel, "r", encoding="utf-8") as f:
                    try:
                        incorrect_data_spiegel = json.load(f)
                    except json.JSONDecodeError:
                        incorrect_data_spiegel = {}
            else:
                incorrect_data_spiegel = {}

            incorrect_data_spiegel.setdefault(issue_number_input, {}).setdefault("article_data_dict", {})[article_key] = f"Die Nummer des Artikels [{article_title}] fehlt."
            logging.warning(f"{indexing} - Artikelnummer fehlt bei [{article_title}] in Ausgabe [{issue_number_input}].")

        # Ueberspringe fehlerhafte Artikel und speichere sie ab (Zielt weniger auf leere Artikel ab und mehr auf falschen html-Code).
        if not all([article_title, article_url, article_text]):

            if os.path.exists(incorrect_data_path_spiegel):
                with open(incorrect_data_path_spiegel, "r", encoding="utf-8") as f:
                    try:
                        incorrect_data_spiegel = json.load(f)
                    except json.JSONDecodeError:
                        incorrect_data_spiegel = {}
            else:
                incorrect_data_spiegel = {}

            logging.warning(f"{indexing} - Ueberspringe Artikel [{article_file_input}]: Fehlende Daten - Title: {article_title}, URL: {article_url}, Text Laenge: {len(article_text) if article_text else 0}")
            key_for_error_naming = f"{article_title} ({article_file_input})" if article_title else article_file_input
            incorrect_data_spiegel.setdefault(issue_number_input, {}).setdefault("article_data_dict", {})[key_for_error_naming] = f"Fehlende oder fehlerhafte Artikeldaten. Siehe: [{article_url}]"

        # Metadaten zuweisen.
        article_data_dict[article_key] = {
            "article_title": article_title,
            "article_subtitle": article_subtitle,  
            "article_kicker": article_kicker,
            "article_number": article_number,
            "article_url": article_url,
            "article_publication_date": article_publication_date.isoformat() if article_publication_date else None,  # Um es als string weiterzugeben und nicht als datetime-Objekt zu verarbeiten.
            "author": article_authors,  # Fuege die Autoreninformationen hinzu
            "article_category": article_category,  # Fuege die Rubrik hinzu
            "keywords": keywords,  # Fuege die Schlagwoerter hinzu
            "is_reading_time": reading_time_bool,
            "reading_time": reading_time,
            "is_copyrighted": copyright_bool,
            "is_paywall": False,
            "is_comment": comments_bool,  # Kommentarfunktion hinzufuegen
            "is_button_like": False,
            "is_button_save": bookmark_button_bool,
            "is_button_copy_link": copylink_button_bool,
            "is_button_send_email": email_link_bool,
            "platforms_sharing": platform_list if platform_list else None, # ueberpruefen, ob die Liste einen Inhalt besitzt
            "is_advertisement": True,
            "date_of_last_update": last_updated_date,
            "word_count": word_count,
            "character_count_with_whitespaces": char_count,
            "article_text": article_text
        }
        # Fuege Logging fuer Artikel hinzu.
        if spec_log:
            logging.debug(f"{indexing} - Extrahierter Artikel: [{article_file_input}]")

    except FileNotFoundError as e:
        logging.error(f"{indexing} - Datei nicht gefunden [{article_file_input}]: {e}")
        # Damit bei einer fehlerhaften Artikelverarbeitung nicht alles gestoppt wird, muss es weiterhin eine Rueckgabe geben.
        incorrect_data_spiegel.setdefault(issue_number_input, {}).setdefault("article_data_dict", {})[article_file_input] = f"Fehler: {str(e)}"
        return {}, issue_number_input, article_key, incorrect_data_spiegel
    
    except UnicodeDecodeError as e:
        logging.error(f"{indexing} - Encoding-Fehler in Datei [{article_file_input}]: {e}")
        incorrect_data_spiegel.setdefault(issue_number_input, {}).setdefault("article_data_dict", {})[article_file_input] = f"Fehler: {str(e)}"
        return {}, issue_number_input, article_key, incorrect_data_spiegel
    
    except Exception as e:
        logging.error(f"{indexing} - Unerwarteter Fehler beim Verarbeiten von [{article_file_input}]: {e}")
        incorrect_data_spiegel.setdefault(issue_number_input, {}).setdefault("article_data_dict", {})[article_file_input] = f"Fehler: {str(e)}"
        return {}, issue_number_input, article_key, incorrect_data_spiegel

    if spec_log:
        logging.debug(f"{indexing} - Extrahierte Artikel fuer [{issue_number_input}]: {article_data_dict.keys()}")

    return article_data_dict, issue_number_input, article_key, incorrect_data_spiegel


def sort_issues_spiegel(output_directory_input, spec_log, indexing, incorrect_data_path_spiegel_input): #-> Wartbarkeit
    """
    Diese Funktion sortiert nach der vollstaendigen Verarbeitung nochmals alle Ausgaben aktiv.
    Das ist eine Sicherheitsvorkehrung, falls vorher etwas mit der Sortierung schieflief.

    Die Eingabewerte (Parameter) sind mit folgenden Werten versehen:
        output_directory_input, der Ordner, in dem sich die finalen Dateien befinden.
        spec_log, Bedingung fuer Log-Nachrichten, ist sie aktiviert, lassen sich detaillierte Nachrichten im Log auffinden.
        indexing, zur besseren Strukturierung, nummeriert dieser Parameter die Nachrichten.
        incorrect_data_path_spiegel_input, Protokoll, um Fehler zu berichten.
    Ausgabewerte sind hier keine zu finden.
    """

    if os.path.exists(incorrect_data_path_spiegel_input):
        with open(incorrect_data_path_spiegel_input, "r", encoding="utf-8") as file:
            try:
                incorrect_data_spiegel = json.load(file)
            except json.JSONDecodeError:
                logging.warning(f"{indexing} - Fehlerprotokoll konnte nicht geladen werden - Datei leer oder beschaedigt")
    else:
        incorrect_data_spiegel = {}

    for json_file in os.listdir(output_directory_input):
        if not json_file.endswith(".json") or json_file in ["spiegel_incorrect_data.json", "spiegel_00_data_default.json", "SPIEGEL_issues_and_articles_def_1.json"] or "stern" in json_file.lower():
            continue

        json_path = os.path.join(output_directory_input, json_file)

        try:
            with open(json_path, "r", encoding="utf-8") as f:
                year_data = json.load(f)

            if not year_data:
                logging.critical(f"{indexing} - Fehlerhafte Jahresdatei [{json_file}]. Leer.")
                incorrect_data_spiegel[json_file] = {
                                "timestamp": datetime.now().isoformat(),
                                "reason": "empty JSON"
                            }
                continue
                
            # z. B. "Der Spiegel - 1950"
            year_key = next(iter(year_data.keys()))
            year_content = year_data[year_key]

            # Trenne die Metadaten und die eigentlichen Ausgaben.
            general_metadata = year_content.get("general_metadata", {})
            issues_only = {
                k: v for k, v in year_content.items()
                if k != "general_metadata"
            }

            # Sortiere die Ausgaben nach ihrer Heftnummer (also nach dem zweiten Teil von z.B. "1950-4").
            sorted_issues = OrderedDict(
                sorted(
                    issues_only.items(), 
                    key=lambda item: (
                        int(item[0].split("-")[0]), 
                        int(item[0].split("-")[1])
                        )
                    )
                )

            # Baue die Jahresstruktur wieder zusammen.
            year_data[year_key] = {
                "general_metadata": general_metadata,
                **sorted_issues
            }

            # ueberschreibe die Datei mit der sortierten Version.
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(year_data, f, indent=4, ensure_ascii=False)

            if spec_log:
                logging.info(f"{indexing} - Jahresdatei [{json_file}] wurde erfolgreich sortiert.")

        except Exception as e:
            logging.error(f"{indexing} - Fehler beim Sortieren der Datei [{json_file}]: {e}")

    # Fehlerprotokoll speichern.
    with open(incorrect_data_path_spiegel_input, "w", encoding="utf-8") as file:
        json.dump(incorrect_data_spiegel, file, indent=4, ensure_ascii=False)


def add_file_size_to_output_file_spiegel(output_directory_input, spec_log, indexing):
    """
    Fuegt jeder Datei die Dateigroeße hinzu.
    Parameter 1: output_directory_input, Der Ordner der finalen Dateien.
    Parameter 2: spec_log, Bedingung fuer detailliertere Log-Nachrichten.
    Parameter 3: indexing, Indexierung zur Nummerierung der Log-Nachrichten.
    Ausgabe: Vorhanden, aber ohne Wert.
    """

    for filename in (f for f in os.listdir(output_directory_input) if re.match(r"^spiegel-\d{4}\.json$", f, re.IGNORECASE)):
        try:
            file_path = os.path.join(output_directory_input, filename)
        
            # Dateigroeße in KB ermitteln.
            output_file_size_kb = os.path.getsize(file_path) // 1024
        
            # JSON oeffnen.
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        
            # Jahr aus dem Inhalt extrahieren.
            # Annahme: Es gibt nur einen Top-Level-Key, z. B. "Der Spiegel - 1976".
            spiegel_key = next(iter(data))
        
            # Einfuegen der Dateigroeße in general_metadata.
            data[spiegel_key]["general_metadata"]["file_size_in_kibibyte"] = output_file_size_kb
        
            # Zurueckschreiben der aktualisierten Datei.
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            print(f"file size added to {file_path}.")
        
            if spec_log:
                logging.info(f"{indexing} - Die Dateigroesse wurde erfolgreich an die Datei [{file_path}] angehaengt.")

        except (json.JSONDecodeError, KeyError, OSError) as e:
            logging.error(f"{indexing} - Fehler beim Anfuegen der Dateigroesse von [{filename}]: {e}")
            return


def get_year_wise_input_data(input_directory_input, spec_log, indexing="3.4"):
    """
    Parameter 1: input_directory_input, Ordner der Rohdateien.
    Parameter 2: spec_log, Bedingung fuer detailliertere Log-Nachrichten.
    Argument: indexing, Indexierung zur Nummerierung der Log-Nachrichten.
    Ausgabe: issue_paths, ein geschateltes Dictionary mit Inhalt die einzelnen Ausgabennummern
    """

    issue_paths = defaultdict(dict)

    for issue_folder in sorted(os.listdir(input_directory_input)):

        # Einzelne Jahresdaten herausholen.
        issue_match = re.search(r"spiegel_(\d{4})_(\d+)", issue_folder)

        if spec_log:
            logging.info(f"{indexing} - Die Input-Datei wurde in [{issue_folder}] gefunden.")

        if not issue_match:
            logging.warning(f"{indexing} - Keine Ausgabendatei des richtigen Formats gefunden.")
            continue

        year, number = map(int, issue_match.groups())

        # Nochmal extra zusammenbauen, damit keine falschen Dateinamen uebertragen werden. --> Robustheit
        issue_paths[year][number] = issue_folder
    return issue_paths


def build_worktable_issues_spiegel(input_directory_input, year, issue_paths, spec_log, indexing="3.4"):
    """
    Ein Worktable, das die Ausgabenmetadaten schrittweise spaeter der Parallelisierung zufuehrt.
    Eingabewerte (Parameter/ Argumente):
        input_directory_input, 
        year, Jahr der zugehoerigen Ausgaben.
        issue_paths, das geschachtelte Dictionary.
        spec_log, Bedingung fuer detailliertere Log-Nachrichten.
        Argument: indexing, Indexierung zur Nummerierung der Log-Nachrichten.
    Ausgabewerte (die mit yield abgerufen werden, es wird also pausiert, bis die for-Schleife ueber dem Funktionsaufruf wieder zurueckkehrt): 
        issue_value, Wert der Ausgabe, also Ausgaben-Metadaten und Artikel.
        issue_date, Ausgabendatum.
        reading_time_by_url, Lesedaueranzeige zur Verarbeitung als Artikel-Metadatum.
        issue_number, Ausgabennummer.
        issue_folder_path, Ordnerpfad.
        issue_html_path, Dateipfad der Ausgabe.
    """

    for number, issue_folder in issue_paths.items():

        issue_number = f"{year}-{number:02d}"
        issue_html_path = os.path.join(input_directory_input, issue_folder, "index.html")
        issue_folder_path = os.path.join(input_directory_input, issue_folder)

        # Funktion fuer Ausgabendaten, die wir fuer die Artikelparallelisierung brauchen, abrufen.
        issue_value, issue_date, reading_time_by_url = extract_issue_metadata_spiegel(issue_html_path, issue_number, indexing)

        yield issue_value, issue_date, reading_time_by_url, issue_number, issue_folder_path, issue_html_path



def build_worktable_articles_spiegel(args):
    """
    Hier kann wegen dem Generator kein automatisches Indexing hinzugefuegt werden. 
    Also das ginge bestimmt irgendwie, aber wuerde zu lange dauern.
    Funktion holt sich Metadaten einer Ausgabe und gibt diese wieder zurueck, wodurch sie unabhaengig und parallelisierbar sind.

    Parameter: Die gleichen wie in build_worktable_issues_spiegel ausgegeben werden.
    Ausgabe (Nach einzelnen Artikeln sortiert.):
        issue_number, Ausgabennummer.
        reading_time_by_url, Metadatum fuer Artikel aus Ausgaben-Website.
        issue_folder_path, Ordnerpfad.
        issue_date, Datum der Ausgabe.
        article_file, die einzelne Artikeldatei.
    """

    spec_log = False

    [issue_value, issue_date, reading_time_by_url, issue_number, issue_folder_path, issue_html_path] = args

    # Artikel der Ausgabe aus dem Ausgabenordner extrahieren und außerdem schon hier alle leeren Artikeldateien aussortieren (Ressourcensparend).
    article_files = [f for f in os.listdir(os.path.join(issue_folder_path)) if f.endswith(".html") and  not re.match(r"^index(\(\d+\))?\.html$", f) and not is_html_empty(os.path.join(issue_folder_path, f), "3.5")]

    if spec_log:
        logging.debug(f"3.5 - Artikel-Dateien gefunden fuer [{issue_number}]: {article_files}")
    
    # Falls keine Artikel in der Ausgabe sind, wird das in die Fehler-Datei gespeichert.
    if not article_files:
        incorrect_data_spiegel.setdefault(issue_number, {})["Issue"] = "Keine Artikel in Ausgabe gefunden."
        logging.warning(f"3.5 - Keine Artikel fuer Ausgabe [{issue_number}] gefunden.")

    # ueber jeden einzelnen Artikel iterieren, der in der Liste steht.
    return [(issue_number, reading_time_by_url, issue_folder_path, issue_date, article_file) for article_file in article_files]


# Fuer 3.5 Parallelisierung Stack-Trace-Analyse. #-> Wartbarkeit

def full_stack():
    """
    Detaillierte Fehleranalyse ausgegeben bekommen, falls die Parallelisierung schief geht.
        Parameter: Keine.
        Ausgabe: Stackstring.
    """

    # Holt den aktuellen Ausnahmetyp (Falls Exception vorhanden ist).
    exc = sys.exc_info()[0]

    # Wenn Fehler existiert.
    if exc is not None:

        # Holt das Traceback-Objekt des vorherigen Stack-Frames.
        f = sys.exc_info()[-1].tb_frame.f_back

        # Nur den Stack der Fehlerstelle holen.
        stack = traceback.extract_stack(f)

    # Wenn nicht, trotzdem letzten Stack-Frame anzeigen lassen. 
    # Da ja die Funktion nur waehrend einer Exception gerufen werden kann (Zumindest in dieser Code-Implementierung).
    else:
        # Nicht alle, letzter waere Full Stack.
        stack = traceback.extract_stack()[:-1]

    # Standardkopf fuer Python-Traceback.
    trc = "Traceback (most recent call last):\n"
    stackstr = trc + "".join(traceback.format_list(stack))

    # Stackliste formatiert an Stackstring haengen.
    if exc is not None:
        stackstr += "  " + traceback.format_exc().lstrip(trc)

    return stackstr


## 3.1 - Die Daten werden nun bereinigt und zu JSON-Dateien verarbeitet.

def extract_spiegel_data(input_directory, output_directory, output_folder, spec_log): 
    """
    Extrahiert alle Ausgaben- und Artikelmetadaten ueber Parallelisierung.
    Parameter:
        input_directory, also das Verzeichnis der Rohdaten, aus dem die Daten und Metadaten extrahiert werden muessen.
        output_directory, das Verzeichnis, in das die finalen Daten gespeichert werden sollen.
        output_folder, das Verzeichnis data.
        spec_log, Bedingung fuer detailliertere Log-Nachrichten.
    Ausgabe: output_directory, das Verzeichnis, in dem die vinalen JSON-Dateien gespeichert werden.
    """

    logging.debug(f"3.1 - Starte Extraktion in [{input_directory}], speichere nach [{output_directory}]")

    # Sicherstellen, dass das Output-Verzeichnis existiert.
    os.makedirs(output_folder, exist_ok=True)
    os.makedirs(output_directory, exist_ok=True)

    # Der Vollstaendigkeit halber aufrufen, was wichtig ist, um z.B. UnboundLocalErrors zu vermeiden.
    incorrect_data_spiegel = {}

    ## 3.2 - Nochmals die Fehlerprotokolle ueberpruefen.

    # Versuchen, die Datei zu oeffnen.
    if os.path.exists(incorrect_data_path_spiegel): #-> Robustheit
        try:
            with open(incorrect_data_path_spiegel, "r", encoding="utf-8") as incorrect_file:
                incorrect_data_spiegel = json.load(incorrect_file)

        except json.JSONDecodeError:
            logging.error(f"3.2 - JSON-Fehler in [{incorrect_data_path_spiegel}], erstelle neue Datei.")
            incorrect_data_spiegel = {}

        except Exception as e:
            logging.error(f"3.2 - Fehler beim Laden der fehlerhaften Daten: [{e}]")
            incorrect_data_spiegel = {}

    else:
        incorrect_data_spiegel = {}
    
    # Sicherstellen, dass `incorrect_data` gueltiges JSON bleibt und auf den vorher schon vorgegebenen Pfad speichern. #-> Robustheit
    try:
        with open(incorrect_data_path_spiegel, "w", encoding="utf-8") as incorrect_file:
            json.dump(incorrect_data_spiegel, incorrect_file, indent=4, ensure_ascii=False)

            if spec_log:
                logging.info(f"3.2 - Datei fuer fehlerhafte Daten [{incorrect_data_path_spiegel}] wurde erfolgreich angelegt.")

    except Exception as e:
        logging.error(f"3.2 - Fehler beim Speichern der fehlerhaften Daten: [{e}]")

    ## 3.3 - Grundsaetzliche Variablen und Dateien festlegen.

    # Eine Liste aller JSON-Dateien erstellen, die im Output-Ordner existieren, damit dadurch der ueberschreibschutz erstellt werden kann.
    # Zwar wird das hier nirgendswo aufgerufen, wenn ich es loesche funktioniert der Code aber nicht mehr, also einfach lassen!
    existing_files = [f for f in os.listdir(output_directory) if f.endswith(".json")]

    all_issue_paths = get_year_wise_input_data(input_directory, spec_log=False)

    ## 3.4 - Die Metadaten der Ausgaben extrahieren und zwischenspeichern (Zeiteffizient, statt RAM-effizient, wie vom Erstbetreuer gefordert.). #-> Effizienz

    # Beginn einer neuen Multiprocessing-Anwendung: Neuer Python-Interpreter-Process mit eigenem Prozesspool gestartet.
    with get_context("spawn").Pool() as pool:

        for year, this_year_paths in all_issue_paths.items():
            
            # Das neue Jahr intialisieren.
            year_output_path = os.path.join(output_directory, f"spiegel-{year}.json")
            
            if os.path.exists(year_output_path):
                logging.warning(f"3.4 - Datei [{year_output_path}] existiert bereits. Ueberspringe Jahrgang {year}.")
                continue
            logging.info(f"3.4 - Aggregieren von Jahrgang {year}")

            # Allgemeine Metadaten definieren.
            general_metadata = {
                "data_scraping_date": datetime.today().strftime('%Y-%m-%d'),
                "scraper_name": "Matthias Endres",
                "institution_name": "Humboldt-Universitaet zu Berlin",
                "notes_on_general_data_en": "article_text: The text is saved with its HTML-formatting-elements. At the end of the text there are also sometimes captions from pictures in the printed issues. Mostly the accompanying pictures are missing on the website. || word_count: Every whitespace and every punctuation mark is used as a seperator. So it isn't very accurate. For example: The german number '500.000' (english: 500,000) is counted as '500' and '000', so as two words. ",
                "notes_on_general_data_de": "article_text: Der Text wird mit den HTML-Formatierungs-Elementen gespeichert. Ebenso sind am Textende manchmal Bildunterschriften aus den urspruenglichen Druckausgaben vorhanden. Die dazugehoerigen Bilder fehlen allerdings meistens auf den Websites. || word_count: Jedes Leerzeichen und Satzzeichen werden als Trennzeichen verwendet. Daher ist die Zaehlung nicht sehr akkurat. Zum Beispiel: Die Zahl '500.000' wird als '500' und '000' gezaehlt, also als zwei Woerter.", 
                "notes_on_specific_data_en": "Following issues are nonexistent in this textcorpus, because they are part of double-issues. Sometimes two issues were released as one bigger, binded together, which isn't represented in the HTML. Following issues are affected: 1948-03 (part of 1948-02), 1960-02 (part of 1960-01), 1962-02 (part of 1962-01), 1963-02 (part of 1963-01), 1964-02 (part of 1964-01), 1965-02 (part of 1965-01), 1966-02 (part of 1966-01), 1967-02 (part of 1967-01), 1969-02 (part of 1969-01), 1970-02 (part of 1970-01), 1971-02 (part of 1971-01), 1972-02 (part of 1972-01), 1974-02 (part of 1974-01), 1975-02 (part of 1975-01), 1976-02 (part of 1976-01), 1976-21 (part of 1976-20), 1977-02 (part of 1977-01), 1978-13 (part of 1978-12), 1980-02 (part of 1980-01), 1981-02 (part of 1981-01).",
                "notes_on_specific_data_de": "Folgende Ausgaben existieren nicht im Textkorpus, da sie Teil von Doppelausgaben waren. Manchmal wurden zwei Ausgaben als eine gebundene veroeffentlicht, was nicht im HTML repraesentiert ist. Folgende sind betroffen: 1948-03 (Teil von 1948-02), 1960-02 (Teil von 1960-01), 1962-02 (Teil von 1962-01), 1963-02 (Teil von 1963-01), 1964-02 (Teil von 1964-01), 1965-02 (Teil von 1965-01), 1966-02 (Teil von 1966-01), 1967-02 (Teil von 1967-01), 1969-02 (Teil von 1969-01), 1970-02 (Teil von 1970-01), 1971-02 (Teil von 1971-01), 1972-02 (Teil von 1972-01), 1974-02 (Teil von 1974-01), 1975-02 (Teil von 1975-01), 1976-02 (Teil von 1976-01), 1976-21 (Teil von 1976-20), 1977-02 (Teil von 1977-01), 1978-13 (Teil von 1978-12), 1980-02 (Teil von 1980-01), 1981-02 (Teil von 1981-01).",
                "file_size_in_kibibyte": None
            }

            # issue-metadaten hier extrahieren.
            # lesen Generator, schreiben Ergebnisse in Liste.
            issue_list = list(build_worktable_issues_spiegel(input_directory, year, this_year_paths, spec_log=False))

            # List comprehension zu einem dict: Die Daten, die nicht fuer Artikel gebraucht werden, herausziehen.
            # Key: issue_number, value: data-dict.
            dict_issue_number_value = dict([(issue[3],issue[0]) for issue in issue_list])

            logging.debug(f"3.4 - Metadaten der Ausgaben erfolgreich gespeichert.")

            ## 3.5 - Parallelisierung der Artikeldaten und Metadaten. #-> Effizienz

            try:
                logging.info(f"3.5 - Parallelisierung der Artikel beginnt.")
                # Lesen Liste in Funktion aus und wandeln sie zu Generator um (in dem sind Tupel), das wird wieder zur Liste.

                # Wurde bei der Durchfuehrung des Codes von Uniseite geaendert. Da es funktioniert werde ich einen Teufel tun und das wieder zu rein multiprocessing umaendern.
                # Wirkt nicht sehr elegant, weil zwei Pools eroeffnet werden. Aber der Lehrstuhl wird das besser koennen, als ich.
                # Einen Prozesspool mit 96 Prozessen erstellen --> Auch moeglich, einfach alle Prozessoren, außer 4 oder wie viele auch immer zu nutzen. #-> Wiederverwendbarkeit (/Portierbarkeit)
                total_cores = os.cpu_count() #!!! Debug-Zwecke 4: spaeter wieder auskommentieren.
                usable_cores = max(1, total_cores - 2) #!!! Debug-Zwecke 4: spaeter wieder auskommentieren.
                # executer = ProcessPoolExecutor(96) #!!! Debug-Zwecke 5:  das hier wieder einfuegen fuer normalen Lauf (Wenn es denn 96 Prozesse gibt. Da muss man gucken.): executer = ProcessPoolExecutor(96)
                executer = ProcessPoolExecutor(usable_cores) #!!! Debug-Zwecke 4

                # Die Generatoren (/Tuples) in Listen umwandeln.
                issue_list = map(list,issue_list)

                # Eine Chronologie erstellen --> worktable blockiert, bis alle anderen Aufgaben durch sind.
                worktable, _ = concurrent.futures.wait([executer.submit(build_worktable_articles_spiegel,i) for i in issue_list])

                # Alle Ergebnisse des Worktables sammeln.
                worktable = [y for x in worktable for y in x.result()]

                logging.info(f"3.5 - Worktable erstellt.")

                # Hier werden alle Artikel parallel durch die extract_article_metadata Funktion gejagt.
                # hier eine progressbar zu machen ist zwar moeglich, aber etwas kompliziert, daher wird es einfach weggelassen.
                # Jeder Aufruf der Funktion bekommt ein Element des worktables.
                results_parallelization = pool.starmap(extract_article_metadata_spiegel, worktable)

            except Exception as e:
                logging.critical(f"3.5 - Schwerwiegender Fehler in der Parallelverarbeitung: {e}")
                logging.critical(full_stack())
                results_parallelization = []
            

            logging.info(f"3.5 - Parallelisierung der Artikel endet.")

            # Defaultdict wird hier genutzt, um fuer jede issue_number gleich alle Artikel zu sammeln.

            for article_data_dict, issue_number, article_key, local_errors in results_parallelization:

                # Alle Fehler zentral zusammenfuehren.
                for k, v in local_errors.items():
                    incorrect_data_spiegel.setdefault(k, {}).update(v)

                if not article_data_dict:
                    logging.warning(f"3.5 - Leerer Artikel [{article_key}] in Ausgabe [{issue_number}] gefunden.")
                    incorrect_data_spiegel.setdefault(issue_number, {})["Article"] = "Leerer Artikel gefunden."
                    continue  # Fehlerhafter oder leerer Artikel → ueberspringen.

                # Artikelstruktur initialisieren, falls noch nicht vorhanden.
                if "article" not in dict_issue_number_value[issue_number]:
                    dict_issue_number_value[issue_number]["article"] = {}
                dict_issue_number_value[issue_number]["article"][article_key] = article_data_dict[article_key]
                
            if spec_log:
                logging.info(f"3.5 - [{len(results_parallelization)}] Artikel wurden verarbeitet.")

            ## 3.6 - Die Daten den JSON-Dateien richtig uebergeben.

            dict_issue_number_value["general_metadata"] = general_metadata
            year_data_spiegel = {f"Der Spiegel - {year}": dict_issue_number_value}

            # Artikel innerhalb der Ausgabe sortieren.
            for issue_number, issue_data_dict in dict_issue_number_value.items():

                if isinstance(issue_data_dict, dict) and "article" in issue_data_dict:

                    try:
                        sorted_articles = dict(sorted(issue_data_dict["article"].items(), key=lambda x: list(map(int, re.search(r"(\d+)-(\d+)$", x[0]).groups()))))
                        issue_data_dict["article"] = sorted_articles

                    except Exception as e:
                        logging.warning(f"3.6 - Artikel konnten nicht sortiert werden: {e}")

            # Die Ausgabe speichern.
            with open(year_output_path, "w", encoding="utf-8") as f:
                json.dump(year_data_spiegel, f, indent=4, ensure_ascii=False)
                logging.info(f"3.6 - Datei [{year_output_path}] wurde gespeichert.")

    # Das inzwischen wieder vereinte Fehlerprotokoll abspeichern.
    with open(incorrect_data_path_spiegel, "w", encoding="utf-8") as f:
        json.dump(incorrect_data_spiegel, f, indent=4, ensure_ascii=False)

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
    log_file = os.path.join(output_folder_local, "process_SPIEGEL.log")
    
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
    
    
    ### 1.0 Die URLs des Spiegels fuer Ausgaben und Artikel herunterladen und in eine JSON-Datei packen.

    print(f"Start: 1 - URLs des Spiegel herausfinden. Datum: {str(datetime.now())}")
    
    # crawl_spiegel_archiv_for_issue_and_article_urls(url_spiegel_archive_default, output_folder_local, spec_log=False)

    print(f"1 - URLs des Spiegels erfolgreich extrahiert. Datum: {str(datetime.now())}")
    
    ### 2.0 - Download der Spiegel-Inhalte
    
    print(f"2: HTML-Rohdaten des Spiegels herunterladen (Ausgaben und Artikel). Datum: {str(datetime.now())}")
    
    # Beispielaufruf der Funktion (anpassen auf deine Pfade):
    # download_articles_by_issue_file_spiegel(input_file=os.path.join(output_folder_local, "SPIEGEL_issues_and_articles_def_1.json"), output_folder=output_folder_local, spec_log=False)
    
    ### 3 - Daten und Metadaten der Ausgaben und Artikel extrahieren und in JSON-Dateien speichern.
    
    print(f"3: Alle Ausgaben und Artikel des Spiegels bereinigen und in finale JSON-Dateien speichern. Datum: {str(datetime.now())}")
    
    ## 3.0 - Ein Fehlerprotokoll fuer kaputte oder fehlerhafte Dateien erstellen.
    
    ## Versuchen, die Datei zu oeffnen (Dabei wegen Parallelisierung vorsichtig sein).
    # Da hier parallelisiert wird, wird in diesem Fall auf geschachtelte Funktionen verzichtet, um Picklingfehler und andere Probleme der Parallelisierung zu vermeiden.
    if os.path.exists(incorrect_data_path_spiegel): #-> Robustheit
        if os.path.getsize(incorrect_data_path_spiegel) > 0:
            try:

                with open(incorrect_data_path_spiegel, "r", encoding="utf-8") as incorrect_file:

                    content = incorrect_file.read().strip()
                    if content:
                        incorrect_data_spiegel = json.loads(content)

                    else:
                        logging.warning(f"3.0 - Datei [{incorrect_data_path_spiegel}] ist inhaltlich leer – neue leere Datenstruktur wird erstellt.")
                        incorrect_data_spiegel = {}

            except json.JSONDecodeError as e:
                logging.error(f"3.0 - JSON-Fehler in [{incorrect_data_path_spiegel}]: [{e}], erstelle neue Datei.")
                incorrect_data_spiegel = {}

            except Exception as e:
                logging.error(f"3.0 - Fehler beim Laden der fehlerhaften Daten: [{e}]")
                incorrect_data_spiegel = {}
        else:
            logging.warning(f"3.0 - Datei [{incorrect_data_path_spiegel}] ist leer, erstelle neue Datei.")
            incorrect_data_spiegel = {}
    else:
        incorrect_data_spiegel = {}
    
    # Sicherstellen, dass `incorrect_data` gueltiges JSON bleibt und auf den vorher schon vorgegebenen Pfad speichern. #-> Robustheit
    os.makedirs(os.path.dirname(incorrect_data_path_spiegel), exist_ok=True)
    
    try:
        with open(incorrect_data_path_spiegel, "w", encoding="utf-8") as incorrect_file:
            json.dump(incorrect_data_spiegel, incorrect_file, indent=4, ensure_ascii=False)
            logging.info(f"3.0 - Datei fuer fehlerhafte Daten [{incorrect_data_path_spiegel}] wurde erfolgreich angelegt.")

    except json.JSONDecodeError as e:
        logging.error(f"3.0 - JSON-Fehler in [{incorrect_data_path_spiegel}]: [{e}], erstelle neue Datei.")
        incorrect_data_spiegel = {}

    except Exception as e:
        logging.error(f"3.0 - Fehler beim Speichern der fehlerhaften Daten: [{e}]")

    # Hier endlich Daten extrahieren.
    extract_spiegel_data(input_directory=os.path.join(output_folder_local, "www.spiegel.de"), output_directory=os.path.join(output_folder_local, "spiegel_json_data_nach_jahren"), output_folder=output_folder_local, spec_log=False)
    
    # Zum Ende nochmal alle Ausgaben in allen Jahresdateien sortieren. #-> Wiederverwendbarkeit
    sort_issues_spiegel(output_directory_input=os.path.join(output_folder_local, "spiegel_json_data_nach_jahren"), spec_log=False, indexing="3.7", incorrect_data_path_spiegel_input=incorrect_data_path_spiegel)
    logging.info("3.7 - Nochmals alle Ausgaben sortiert.")

    # Und schließlich noch einmal die Dateigroeße jeder Datei anfuegen.
    add_file_size_to_output_file_spiegel(output_directory_input=os.path.join(output_folder_local, "spiegel_json_data_nach_jahren"), spec_log=False, indexing="3.8")
    logging.info("3.8 - Dateigroessen dem JSON angehaengt.")

    logging.info("4 - Code fuer Spiegel abgeschlossen.")
    print(f"4 - Code fuer Spiegel abgeschlossen. Datum: {str(datetime.now())} (Achtung, wegen Multiprocessing bitte trotzdem noch eine Stunde lang den Code nicht anfassen, da diese Nachricht normalerweise schon beim ersten Prozess gedruckt wird.")


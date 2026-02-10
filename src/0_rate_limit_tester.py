"""
Rate-Limit-Tester. Schauen, wie sich die Botdetection der einzelnen Webseiten verhält, um entsprechend darauf im Web Scraping reagieren können.
Teil der Codesammlung der Masterarbeit von Matthias Endres.
"""

# 0 - Imports.

import requests
from bs4 import BeautifulSoup
import os
import random
import time
from datetime import datetime

# 1 - Beispiel-Webseiten.

url = "https://www.spiegel.de/"
# url = "https://www.stern.de"


# 2 - Rate-Limit WIRKLICH ausreizen.

MAX_REQUESTS = 30000

failed_requests = []

for i in range(MAX_REQUESTS):
    try:
        response = requests.get(url)
        print(f"[{i + 1}/{MAX_REQUESTS}]| Zeit: [{str(datetime.now())}]| Status-Code: {response.status_code}")

        # Optional: Wenn Fehler (z. B. 429 Too Many Requests)
        if not (200 <= response.status_code <= 299):
            failed_requests.append((i + 1, response.status_code))

    except Exception as e:
        print(f"[{i + 1}/{MAX_REQUESTS}] ❌ Fehler bei der Anfrage: {e}")
        failed_requests.append((i + 1, str(e)))

print("\n=== Test abgeschlossen ===")
print(f"Gesendete Anfragen: {MAX_REQUESTS}")
print(f"Fehlgeschlagene Anfragen: {len(failed_requests)}")

if failed_requests:
    print("\nFehlerliste:")
    for index, error in failed_requests:
        print(f"#{index}: {error}")


"""
# 2 - Rate-Limit ausreizen (sowohl gleiche Seite, als auch verschiedene)

# Start-Delay 2, Min-delay 0,1 und decrement 0,3 sind ohne Probleme durchgelaufen
# Start-Delay 0,4, Min-Delay 0,05 und decrement 0,05 sind ohne Probleme durchgelaufen
# Start-Delay 0,1, Min-Delay 0,001 und decrement 0,005

def test_rate_limit_single_url(url, start_delay, decrement, min_delay, requests_per_interval):
    '''
    Testet das Rate-Limit einer Webseite für eine einzelne URL, indem Anfragen mit immer kürzeren Verzögerungen gesendet werden.
    
    :param url: Die URL, die getestet werden soll.
    :param start_delay: Startzeitverzögerung in Sekunden zwischen den Anfragen.
    :param decrement: Die Menge, um die die Verzögerung nach jeder Serie von Anfragen reduziert wird.
    :param min_delay: Die minimale Verzögerung in Sekunden zwischen den Anfragen.
    :param requests_per_interval: Anzahl der Anfragen pro Zeitintervall.
    '''
    delay = start_delay
    failed_requests = []

    # while delay >= min_delay:
    print(f"\nStarte Test mit {requests_per_interval} Anfragen und einer Verzögerung von {delay:.2f} Sekunden.") ####
    for i in range(requests_per_interval):
        try:
            print(f"[{i+1}/{requests_per_interval}] Sende Anfrage an {url}.")
            response = requests.get(url)
            if not (200 <= response.status_code <= 299):
                print(f"Fehler: Status-Code {response.status_code} für {url}")
                failed_requests.append((url, response.status_code))
                return  # Test abbrechen bei Fehler
            print(f"Erfolgreiche Anfrage. Status-Code: {response.status_code}")
            time.sleep(delay)
        except Exception as e:
            print(f"Fehler bei der Anfrage an {url}: {e}")
            failed_requests.append((url, str(e)))
            return  # Test abbrechen bei Fehler

    # Reduziere die Verzögerung für die nächste Serie
    delay -= decrement
    time.sleep(10) ####

    print("\nTest abgeschlossen.")
    if failed_requests:
        print("Fehlgeschlagene Anfragen:")
        for failed in failed_requests:
            print(f"Bei {delay} Sekunden wurde die Anfrage gestoppt")
            print(f"URL: {failed[0]} - Fehler: {failed[1]}")

    else:
        print("Alle Anfragen erfolgreich.")

# Beispiel-URL (fügen Sie die gewünschte Test-URL hier ein)
test_url = "https://www.beispielseite.de"

test_rate_limit_single_url(url, start_delay=0.0, decrement=0.005, min_delay=0.0, max_request=20000)
"""

import os
import csv
import re
from collections import defaultdict, Counter
import matplotlib.pyplot as plt

# Dateipfade

input_path = r"C:\Users\Matze\Notebooks\endres-webscraping\data\research\extrahierte_911_artikel.txt"
csv_path   = r"C:\Users\Matze\Notebooks\endres-webscraping\data\research\schlagwort_frequenzanalyse.csv"
png_path   = r"C:\Users\Matze\Notebooks\endres-webscraping\data\research\schlagwort_frequenzanalyse.png"

# Schlagwort-Definitionen  (beliebige Groß-/Kleinschreibung möglich)

spezifische_raw = {
    "World Trade Center", "WTC", "Twin Towers", "Bin Laden", "Osama",
    "al-Qaida", "terrorzelle", "Ground Zero", "Pentagon",
    "Flugzeugentführung", "Hijacker", "hijacking", "Terroranschlag",
    "Terroristen", "Terrorpilot", "Versa", "George Bush",
    "Krieg gegen den Terror", "CIA", "FBI"
}

zeit_raw   = {"9/11", "9-11", "Nineeleven", "Nine-Eleven"}
datum_raw  = {"11. September", "Elfter September", "September 11", "September elf"}

allgemein_raw = {
    "Anschlag", "Terrorakt", "Angriff", "Attentat", "Aktion",
    "Tat", "Attacke", "Anschläge", "Anschlagsserie", "Terror", "Attentäter"
}


# Alles in Kleinbuchstaben umwandeln  → echte case-insensitive Sets

normalize_kw = lambda s: s.lower()

spezifische_schlagwörter = {normalize_kw(s) for s in spezifische_raw}
zeit_schlagwörter        = {normalize_kw(s) for s in zeit_raw}
datum_schlagwörter       = {normalize_kw(s) for s in datum_raw}
allgemeine_schlagwörter  = {normalize_kw(s) for s in allgemein_raw}
 
def normalize(text: str) -> str:
    """Text vereinheitlichen (klein, Sonderzeichen raus, Umlaute ersetzen)."""
    text = text.lower()
    text = text.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss")
    text = re.sub(r"[^a-z0-9/.\- ]+", " ", text)  # nur Grundzeichen erlauben
    text = re.sub(r"\s+", " ", text)               # doppelte Leerzeichen weg
    return text.strip()

def lade_artikel(pfad: str) -> list[str]:
    if not os.path.exists(pfad):
        raise FileNotFoundError(f"Datei nicht gefunden: {pfad}")
    with open(pfad, encoding="utf-8") as f:
        return [z.strip() for z in f if z.strip()]

def verarbeite_artikel(zeilen: list[str]) -> dict[str, Counter]:
    statistik = defaultdict(Counter)       # jahr -> Counter

    for zeile in zeilen:
        teile = zeile.split(" | ", maxsplit=8)
        if len(teile) < 9:
            continue
        quelle, jahr, *_ , text = teile
        norm = normalize(text)

        # Artikel nur zählen, falls min. 1 spezifisches Schlagwort existiert
        if not any(sw in norm for sw in spezifische_schlagwörter
                                   | zeit_schlagwörter
                                   | datum_schlagwörter):
            continue

        # Zeit-/Datumsgruppen
        if any(sw in norm for sw in zeit_schlagwörter):
            statistik[jahr]["9/11"] += 1
        if any(sw in norm for sw in datum_schlagwörter):
            statistik[jahr]["11. September"] += 1

        # Allgemeine Schlagwörter
        for sw in allgemeine_schlagwörter:
            if sw in norm:
                statistik[jahr][sw] += 1

    return statistik

def schreibe_csv(statistik: dict, pfad: str):
    os.makedirs(os.path.dirname(pfad), exist_ok=True)
    jahre = sorted(statistik)
    spalten = sorted({sw for j in statistik for sw in statistik[j]})

    with open(pfad, "w", encoding="utf-8-sig", newline="") as f:
        wr = csv.writer(f, delimiter=";")
        wr.writerow(["jahr"] + spalten)
        for jahr in jahre:
            wr.writerow([jahr] + [statistik[jahr].get(sw, 0) for sw in spalten])
    print(f"CSV gespeichert: {pfad}")

def zeichne_zeitverlauf(statistik: dict, pfad: str):
    jahre = sorted(statistik)
    spalten = sorted({sw for j in statistik for sw in statistik[j]})

    plt.figure(figsize=(12,6))
    for sw in spalten:
        werte = [statistik[j].get(sw, 0) for j in jahre]
        plt.plot(jahre, werte, marker="o", label=sw)

    plt.title("Schlagwortfrequenz 2001-2009 (nur Artikel mit 9/11-Bezug)")
    plt.xlabel("Jahr"); plt.ylabel("Anzahl Artikel")
    plt.legend(); plt.tight_layout()
    plt.savefig(pfad, dpi=300)
    print(f"PNG gespeichert: {pfad}")

if __name__ == "__main__":
    artikel = lade_artikel(input_path)
    stats   = verarbeite_artikel(artikel)
    schreibe_csv(stats, csv_path)
    zeichne_zeitverlauf(stats, png_path)

import os
import json
import re
from collections import defaultdict
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

# Konfiguration
zieljahre = [str(j) for j in range(2001, 2010)]
spiegel_dir = r"C:\Users\Matze\Notebooks\endres-webscraping\data\spiegel_json_data_nach_jahren"
stern_dir   = r"C:\Users\Matze\Notebooks\endres-webscraping\data\stern_json_data_nach_jahren"
output_csv  = r"C:\Users\Matze\Notebooks\endres-webscraping\data\research\911_monatliche_berichte.csv"
output_plot = r"C:\Users\Matze\Notebooks\endres-webscraping\data\research\911_monatliche_berichte.png"

# Schlagwörter
spezifische_schlagwoerter = {
    "wtc", "world trade center", "twin towers", "osama bin laden",
    "al-qaida", "al quaida", "bin laden", "nineeleven", "nine-eleven",
    "9/11", "9-11", "11. september", "elfter september"
}
spezifisch = [re.compile(rf"\b{re.escape(w)}\b", re.IGNORECASE) for w in spezifische_schlagwoerter]

# Zähler initialisieren
zaehlungen = defaultdict(lambda: {"SPIEGEL": 0, "STERN": 0})

# SPIEGEL verarbeiten
for fname in os.listdir(spiegel_dir):
    if not fname.startswith("spiegel-") or not fname.endswith(".json"):
        continue
    jahr = fname[8:-5]
    if jahr not in zieljahre:
        continue
    with open(os.path.join(spiegel_dir, fname), encoding="utf-8") as f:
        try:
            data = json.load(f)
        except Exception:
            continue

    ausgaben = data.get(f"Der Spiegel - {jahr}", {})
    for ausgabe in ausgaben.values():
        for artikel in ausgabe.get("article", {}).values():
            if not isinstance(artikel, dict):
                continue
            textfelder = [
                artikel.get("article_title") or "",
                artikel.get("article_subtitle") or "",
                " ".join(artikel.get("article_kicker") or []),
                artikel.get("article_text") or ""
            ]

            inhalt = " ".join(textfelder).lower()
            if any(r.search(inhalt) for r in spezifisch):
                datum = artikel.get("article_publication_date", "")
                try:
                    dt = datetime.fromisoformat(datum.split("+")[0])
                    monat = f"{dt.year:04d}-{dt.month:02d}"
                    zaehlungen[monat]["SPIEGEL"] += 1
                except Exception:
                    continue

# STERN

for fname in os.listdir(stern_dir):
    if not fname.startswith("stern-") or not fname.endswith(".json"):
        continue
    jahr = fname[6:-5]
    if jahr not in zieljahre:
        continue
    with open(os.path.join(stern_dir, fname), encoding="utf-8") as f:
        try:
            data = json.load(f)
        except Exception:
            continue

    monate = data.get(f"Stern - {jahr}", {})
    for monat_name, eintraege in monate.items():
        # Beispiel für monat_name: "Stern - 2001 - 02"
        if not isinstance(eintraege, dict):
            continue

        # Monat aus dem String extrahieren mit Regex
        match = re.search(r"(\d{4})\s*-\s*(\d{2})", monat_name)
        if not match:
            continue
        jahr_extrahiert, monat_extrahiert = match.groups()
        monat_key = f"{jahr_extrahiert}-{monat_extrahiert}"

        for eintrag in eintraege.values():
            if not isinstance(eintrag, dict):
                continue
            artikel_dict = eintrag.get("article", {})
            if not isinstance(artikel_dict, dict):
                continue
            for artikel in artikel_dict.values():
                if not isinstance(artikel, dict):
                    continue

                textfelder = [
                    artikel.get("article_title") or "",
                    artikel.get("article_subtitle") or "",
                    artikel.get("article_text") or ""
                ]
                inhalt = " ".join(textfelder).lower()
                if any(r.search(inhalt) for r in spezifisch):
                    zaehlungen[monat_key]["STERN"] += 1



# Datenframe erstellen
df = pd.DataFrame([
    {"monat": monat, "SPIEGEL": werte["SPIEGEL"], "STERN": werte["STERN"]}
    for monat, werte in sorted(zaehlungen.items())
])
# Datum als datetime konvertieren (für korrektes Plotten)
df["monat_dt"] = pd.to_datetime(df["monat"], format="%Y-%m")

# CSV speichern
os.makedirs(os.path.dirname(output_csv), exist_ok=True)
df.to_csv(output_csv, sep=";", index=False, encoding="utf-8-sig")
print(f" CSV gespeichert: {output_csv}")

plt.gca().xaxis.set_major_locator(plt.MaxNLocator(12))


# Diagramm erstellen
plt.figure(figsize=(14, 6))
plt.plot(df["monat_dt"], df["SPIEGEL"], marker="o", label="SPIEGEL")
plt.plot(df["monat_dt"], df["STERN"], marker="s", label="STERN")
plt.xticks(rotation=45)
plt.title("Berichterstattung zu 9/11 pro Monat (2001–2009)")
plt.ylabel("Anzahl Artikel")
plt.xlabel("Monat")
plt.legend()
plt.tight_layout()
plt.savefig(output_plot)
plt.close()
print(f" Plot gespeichert: {output_plot}")

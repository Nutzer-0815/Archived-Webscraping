import os
import json
from collections import defaultdict
import pandas as pd


# Verzeichnisse

spiegel_dir = r"C:\Users\Matze\Notebooks\endres-webscraping\data\spiegel_json_data_nach_jahren"
stern_dir = r"C:\Users\Matze\Notebooks\endres-webscraping\data\stern_json_data_nach_jahren"
output_dir = r"C:\Users\Matze\Notebooks\endres-webscraping\data\research"
os.makedirs(output_dir, exist_ok=True)


# 1. SPIEGEL: Artikelanzahl + Rubriken pro Jahr

artikel_rubriken_pro_jahr_spiegel = defaultdict(lambda: {"gesamt": 0, "rubriken": defaultdict(int)})

for filename in sorted(os.listdir(spiegel_dir)):
    if not filename.endswith(".json") or filename.startswith("0_"):
        continue

    filepath = os.path.join(spiegel_dir, filename)
    with open(filepath, "r", encoding="utf-8") as file:
        try:
            data = json.load(file)
        except json.JSONDecodeError:
            continue

        for jahr_key, ausgaben in data.items():
            jahr = jahr_key.split(" - ")[-1]

            for ausgabe_id, ausgabe_data in ausgaben.items():
                if not isinstance(ausgabe_data, dict):
                    continue

                artikel_dict = ausgabe_data.get("article", {})
                for artikel_id, artikel in artikel_dict.items():
                    artikel_rubriken_pro_jahr_spiegel[jahr]["gesamt"] += 1

                    kategorien = artikel.get("category", []) #! später noch in article_category umwandeln
                    if isinstance(kategorien, list):
                        for kat in kategorien:
                            artikel_rubriken_pro_jahr_spiegel[jahr]["rubriken"][kat] += 1

# In DataFrame umwandeln
rows = []
for jahr, daten in artikel_rubriken_pro_jahr_spiegel.items():
    row = {"Jahr": jahr, "Spiegel_Artikel": daten["gesamt"]}
    row.update(daten["rubriken"])  # jede Rubrik als eigene Spalte
    rows.append(row)

df_spiegel_rubriken = pd.DataFrame(rows)
df_spiegel_rubriken["Jahr"] = df_spiegel_rubriken["Jahr"].astype(int)
df_spiegel_rubriken = df_spiegel_rubriken.sort_values("Jahr")
df_spiegel_rubriken = df_spiegel_rubriken.fillna(0).astype({col: int for col in df_spiegel_rubriken.columns if col != "Jahr"})

# Als CSV speichern
output_spiegel_rubriken_csv = os.path.join(output_dir, "1_artikelanzahl_und_rubriken_pro_jahr_spiegel.csv")
df_spiegel_rubriken.to_csv(output_spiegel_rubriken_csv, index=False, sep=";", encoding="utf-8-sig")

print(f"Spiegel: Artikelanzahl + Rubriken pro Jahr gespeichert unter:\n{output_spiegel_rubriken_csv}")

# 2. STERN: Artikelanzahl + Rubriken pro Jahr

artikel_rubriken_pro_jahr_stern = defaultdict(lambda: {"gesamt": 0, "rubriken": defaultdict(int)})

for filename in sorted(os.listdir(stern_dir)):
    if not filename.endswith(".json") or filename.startswith("0_"):
        continue

    filepath = os.path.join(stern_dir, filename)
    with open(filepath, "r", encoding="utf-8") as file:
        try:
            data = json.load(file)
        except json.JSONDecodeError:
            continue

        for jahr_key, monate in data.items():
            jahr = jahr_key.split(" - ")[-1]

            for monat_key, url_daten in monate.items():
                if not isinstance(url_daten, dict):
                    continue

                for url, artikelblock in url_daten.items():
                    if not isinstance(artikelblock, dict):
                        continue

                    artikel_dict = artikelblock.get("article", {})
                    for artikel_id, artikel in artikel_dict.items():
                        artikel_rubriken_pro_jahr_stern[jahr]["gesamt"] += 1
                        kategorien = artikel.get("article_category", [])
                        if isinstance(kategorien, list):
                            for kat in kategorien:
                                artikel_rubriken_pro_jahr_stern[jahr]["rubriken"][kat] += 1

# In DataFrame umwandeln
rows = []
for jahr, daten in artikel_rubriken_pro_jahr_stern.items():
    row = {"Jahr": jahr, "Stern_Artikel": daten["gesamt"]}
    row.update(daten["rubriken"])  # jede Rubrik als Spalte
    rows.append(row)

df_stern_rubriken = pd.DataFrame(rows)
df_stern_rubriken["Jahr"] = df_stern_rubriken["Jahr"].astype(int)
df_stern_rubriken = df_stern_rubriken.sort_values("Jahr")
df_stern_rubriken = df_stern_rubriken.fillna(0).astype({col: int for col in df_stern_rubriken.columns if col != "Jahr"})

# Als CSV speichern
output_stern_rubriken_csv = os.path.join(output_dir, "1_artikelanzahl_und_rubriken_pro_jahr_stern.csv")
df_stern_rubriken.to_csv(output_stern_rubriken_csv, index=False, sep=";", encoding="utf-8-sig")

print(f"Stern: Artikelanzahl + Rubriken pro Jahr gespeichert unter:\n{output_stern_rubriken_csv}")

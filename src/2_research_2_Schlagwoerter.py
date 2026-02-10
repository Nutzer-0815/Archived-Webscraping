import pandas as pd
import json
import os
from collections import defaultdict

# Ordnerpfade
stern_folder = r'C:\Users\Matze\Notebooks\endres-webscraping\data\stern_json_data_nach_jahren'
spiegel_folder = r'C:\Users\Matze\Notebooks\endres-webscraping\data\spiegel_json_data_nach_jahren'
output_dir = r'C:\Users\Matze\Notebooks\endres-webscraping\data\research'
os.makedirs(output_dir, exist_ok=True)

# Schlagwörter definieren
# Bereits genutzte Keywords (Case-insensitiv):
keywords = ["bin laden"]

# Ergebnislisten
results = []
jahr_stats = defaultdict(lambda: {"Spiegel": 0, "Stern": 0})

# Hilfsfunktion
def count_keywords(text, keywords):
    if not text:
        return 0
    text_lower = text.lower()
    return sum(text_lower.count(kw.lower()) for kw in keywords)


# 1 - STERN-Daten verarbeiten

stern_counter = 1
for file in os.listdir(stern_folder):
    if file.endswith('.json'):
        with open(os.path.join(stern_folder, file), 'r', encoding='utf-8') as f:
            data = json.load(f)

        for year_key, year_data in data.items():
            if not year_key.startswith("Stern"):
                continue

            for month_key, month_data in year_data.items():
                if not isinstance(month_data, dict):
                    continue

                for url_key, url_data in month_data.items():
                    if not isinstance(url_data, dict):
                        continue

                    article_data = url_data.get("article", {})
                    for article_id, article in article_data.items():
                        text = article.get("article_text", "")
                        count = count_keywords(text, keywords)
                        if count > 0:
                            jahr = year_key.split('-')[1].strip()
                            jahr_stats[jahr]["Stern"] += count
                            nummer = f"S{stern_counter:03d}"
                            results.append({
                                "ID": nummer,
                                "Treffer": count,
                                "Magazin": "Stern",
                                "Jahr": jahr,
                                "Titel": article.get("article_title"),
                                "Pfad": f"{year_key}, Kategorie {url_data.get('category', '?')}, Monat {url_data.get('month', '?')}, Seite {url_data.get('page', '?')}, Artikel {article.get('article_number')}"
                            })
                            stern_counter += 1


# 2 - SPIEGEL-Daten verarbeiten

spiegel_counter = 1
for file in os.listdir(spiegel_folder):
    if file.endswith('.json'):
        with open(os.path.join(spiegel_folder, file), 'r', encoding='utf-8') as f:
            data = json.load(f)

        for year_key, year_data in data.items():
            if not year_key.startswith("Der Spiegel"):
                continue

            for issue_key, issue_data in year_data.items():
                if not isinstance(issue_data, dict):
                    continue

                articles = issue_data.get("article", {})
                for article_id, article in articles.items():
                    text = article.get("article_text", "")
                    count = count_keywords(text, keywords)
                    if count > 0:
                        jahr = year_key.split('-')[1].strip()
                        jahr_stats[jahr]["Spiegel"] += count
                        nummer = f"P{spiegel_counter:03d}"
                        results.append({
                            "ID": nummer,
                            "Treffer": count,
                            "Magazin": "Spiegel",
                            "Jahr": jahr,
                            "Titel": article.get("article_title"),
                            "Pfad": f"{year_key}, Ausgabe {issue_key}, Artikel {article.get('article_number')}"
                        })
                        spiegel_counter += 1


# 3 - Ausgabe pro Artikel als CSV (angepasst für leere erste Spalte mit Schlagwort)


# DataFrame vorbereiten
df_results = pd.DataFrame(results)
df_results = df_results.sort_values(by=["Jahr", "Magazin"])

# Schlagwort in Zelle A1, Rest um 1 Spalte verschoben
suchbegriff = keywords[0] if len(keywords) == 1 else ", ".join(keywords)

# CSV-Dateipfad
output_results_csv = os.path.join(output_dir, f"2_ausgabe_schlagwort_{keywords[0]}.csv")

# Datei manuell schreiben, um A1 und Verschiebung zu kontrollieren
with open(output_results_csv, 'w', encoding='utf-8-sig') as f:
    # Kopfzeile: leeres Feld + Spaltennamen
    f.write(f"{suchbegriff};" + ";".join(df_results.columns) + "\n")
    
    # Alle weiteren Zeilen: leere erste Zelle + normale Werte
    for _, row in df_results.iterrows():
        f.write(";" + ";".join(map(str, row.values)) + "\n")

print(f" Artikel mit Schlagwort gespeichert unter:\n{output_results_csv}")



# 4 - Statistik pro Jahr als CSV

statistik_rows = []
for jahr in sorted(jahr_stats.keys(), key=int):
    statistik_rows.append({
        "Jahr": jahr,
        "Spiegel_Treffer": jahr_stats[jahr]["Spiegel"],
        "Stern_Treffer": jahr_stats[jahr]["Stern"]
    })

df_statistik = pd.DataFrame(statistik_rows)
df_statistik["Jahr"] = df_statistik["Jahr"].astype(int)
df_statistik = df_statistik.sort_values("Jahr")
output_stats_csv = os.path.join(output_dir, "2_schlagwort_statistik_pro_jahr.csv")
df_statistik.to_csv(output_stats_csv, index=False, sep=";", encoding="utf-8-sig")

print(f"Jahresstatistik gespeichert unter:\n{output_stats_csv}")


# 5 - Plot als PNG speichern

import matplotlib.pyplot as plt

output_plot = os.path.join(output_dir, "2_schlagwort_statistik_pro_jahr.png")

plt.figure(figsize=(14,7))
plt.plot(df_statistik["Jahr"], df_statistik["Spiegel_Treffer"], marker="o", label="Spiegel")
plt.plot(df_statistik["Jahr"], df_statistik["Stern_Treffer"], marker="s", label="Stern")
plt.title(f'Häufigkeit des Schlagworts "{suchbegriff}" pro Jahr')
plt.xlabel("Jahr")
plt.ylabel("Anzahl Treffer (in Artikeln)")
plt.legend()
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig(output_plot)
plt.close()

print(f"Diagramm gespeichert unter:\n{output_plot}")

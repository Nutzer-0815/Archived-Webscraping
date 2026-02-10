import os
import json
import re
import pandas as pd
from collections import defaultdict
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.pyplot as plt
import numpy as np

spiegel_dir = r"C:\Users\Matze\Notebooks\endres-webscraping\data\spiegel_json_data_nach_jahren"
stern_dir   = r"C:\Users\Matze\Notebooks\endres-webscraping\data\stern_json_data_nach_jahren"
# Zeitraum: von Jan 1947 bis Dez 2025
start_monat = pd.Timestamp("1947-01-01")
end_monat = pd.Timestamp("2025-12-01")

output_csv  = r"C:\Users\Matze\Notebooks\endres-webscraping\data\research\1_artikel_laenge_stats_full.csv"
output_plot_word = r"C:\Users\Matze\Notebooks\endres-webscraping\data\research\1_artikel_laenge_words_full.png"
output_plot_char = r"C:\Users\Matze\Notebooks\endres-webscraping\data\research\1_artikel_laenge_chars_full.png"
output_dir = r"C:\Users\Matze\Notebooks\endres-webscraping\data\research"
os.makedirs(output_dir, exist_ok=True)

# Monatsstempel
alle_monate = pd.date_range(start=start_monat, end=end_monat, freq='MS')
monat_keys = [dt.strftime('%Y-%m') for dt in alle_monate]

werte = {monat: {"SPIEGEL_words": [], "SPIEGEL_chars": [], "STERN_words": [], "STERN_chars": []} for monat in monat_keys}

# SPIEGEL: 1947–2009
for fname in os.listdir(spiegel_dir):
    if not fname.endswith(".json") or not fname.startswith("spiegel-"):
        continue
    jahr = fname[8:-5]
    # Auch Jahre außerhalb des Rahmens überspringen!
    if not jahr.isdigit() or int(jahr) < 1947 or int(jahr) > 2009:
        continue
    with open(os.path.join(spiegel_dir, fname), encoding="utf-8") as f:
        try: data = json.load(f)
        except: continue
    ausgaben = data.get(f"Der Spiegel - {jahr}", {})
    for ausgabe in ausgaben.values():
        for artikel in ausgabe.get("article", {}).values():
            if not isinstance(artikel, dict): continue
            word_count = artikel.get("word_count")
            char_count = artikel.get("character_count_with_whitespaces")
            dat = artikel.get("article_publication_date","")
            try:
                dt = datetime.fromisoformat(dat.split("+")[0])
                monat_key = f"{dt.year:04d}-{dt.month:02d}"
            except:
                monat_key = f"{jahr}-01"
            if monat_key not in werte:
                continue
            try:
                word_count = int(word_count)
            except:
                word_count = None
            try:
                char_count = int(char_count)
            except:
                char_count = None
            if word_count is not None:
                werte[monat_key]["SPIEGEL_words"].append(word_count)
            if char_count is not None:
                werte[monat_key]["SPIEGEL_chars"].append(char_count)

# STERN: 2000–2025
for fname in os.listdir(stern_dir):
    if not fname.endswith(".json") or not fname.startswith("stern-"):
        continue
    jahr = fname[6:-5]
    # Auch Jahre außerhalb des Rahmens überspringen!
    if not jahr.isdigit() or int(jahr) < 2000 or int(jahr) > 2025:
        continue
    with open(os.path.join(stern_dir, fname), encoding="utf-8") as f:
        try: data = json.load(f)
        except: continue
    monate = data.get(f"Stern - {jahr}", {})
    for monatname, eintraege in monate.items():
        m = 1
        m_match = re.search(r"(\d{4})\s*-\s*(\d{2})", monatname)
        if m_match:
            m = int(m_match.group(2))
        monat_key = f"{jahr}-{int(m):02d}"
        if monat_key not in werte:
            continue
        for eintrag in eintraege.values():
            artikel_dict = eintrag.get("article", {}) if isinstance(eintrag, dict) else {}
            for artikel in artikel_dict.values():
                if not isinstance(artikel, dict): continue
                word_count = artikel.get("word_count")
                char_count = artikel.get("character_count_with_whitespaces")
                try:
                    word_count = int(word_count)
                except:
                    word_count = None
                try:
                    char_count = int(char_count)
                except:
                    char_count = None
                if word_count is not None:
                    werte[monat_key]["STERN_words"].append(word_count)
                if char_count is not None:
                    werte[monat_key]["STERN_chars"].append(char_count)

# DataFrame 
daten = []
for monat in monat_keys:
    sp_words = werte[monat]["SPIEGEL_words"]
    sp_chars = werte[monat]["SPIEGEL_chars"]
    st_words = werte[monat]["STERN_words"]
    st_chars = werte[monat]["STERN_chars"]
    daten.append({
        "monat": monat,
        "SPIEGEL_avg_words": sum(sp_words)/len(sp_words) if sp_words else None,
        "SPIEGEL_avg_chars": sum(sp_chars)/len(sp_chars) if sp_chars else None,
        "STERN_avg_words": sum(st_words)/len(st_words) if st_words else None,
        "STERN_avg_chars": sum(st_chars)/len(st_chars) if st_chars else None,
        "SPIEGEL_anzahl": len(sp_words),
        "STERN_anzahl": len(st_words),
    })
df = pd.DataFrame(daten)
df["monat_dt"] = pd.to_datetime(df["monat"], format="%Y-%m")

# Speichern
df.to_csv(output_csv, sep=";", index=False, encoding="utf-8-sig")
print(f"Artikelstatistik CSV gespeichert: {output_csv}")

# Plot: Wörter
plt.figure(figsize=(18,8))
plt.plot(df["monat_dt"], df["SPIEGEL_avg_words"], marker="o", label="SPIEGEL: Ø Wörter")
plt.plot(df["monat_dt"], df["STERN_avg_words"], marker="s", label="STERN: Ø Wörter")
plt.legend()
plt.title("Durchschnittliche Artikellänge (Wörter) pro Monat, 1947–2025")
plt.xlabel("Monat")
plt.ylabel("Ø Wörter pro Artikel")
plt.tight_layout()
plt.savefig(output_plot_word)
plt.close()
print(f"Plot gespeichert: {output_plot_word}")

# Plot: Zeichen
plt.figure(figsize=(18,8))
plt.plot(df["monat_dt"], df["SPIEGEL_avg_chars"], marker="o", label="SPIEGEL: Ø Zeichen")
plt.plot(df["monat_dt"], df["STERN_avg_chars"], marker="s", label="STERN: Ø Zeichen")
plt.legend()
plt.title("Durchschnittliche Artikellänge (Zeichen inkl. Leerzeichen) pro Monat, 1947–2025")
plt.xlabel("Monat")
plt.ylabel("Ø Zeichen pro Artikel")
plt.tight_layout()
plt.savefig(output_plot_char)
plt.close()
print(f"Plot gespeichert: {output_plot_char}")

# Aus df, das monatliche Werte enthält, den Jahresdurchschnitt bilden:
df["jahr"] = df["monat_dt"].dt.year

jahreswerte = df.groupby("jahr").agg({
    "SPIEGEL_avg_words": "mean",
    "SPIEGEL_avg_chars": "mean",
    "STERN_avg_words": "mean",
    "STERN_avg_chars": "mean",
    "SPIEGEL_anzahl": "sum",
    "STERN_anzahl": "sum",
}).reset_index()

# Speichern oder plotten
jahreswerte.to_csv(r"C:\Users\Matze\Notebooks\endres-webscraping\data\research\1_artikel_laenge_stats_jahreswerte.csv", sep=";", index=False, encoding="utf-8-sig")
print("Jahreswerte gespeichert!")


def artikel_stats_pro_jahr_und_rubrik(json_dir, magazin_name, jahr_range, min_anzahl=10):
    # {(rubrik, jahr): [word_count, ...]}
    rubrik_jahr = defaultdict(list)
    rubrik_counts = defaultdict(int)
    for fname in os.listdir(json_dir):
        if not fname.endswith(".json"):
            continue
        with open(os.path.join(json_dir, fname), encoding="utf-8") as f:
            try: data = json.load(f)
            except: continue
        for year_key, year_val in data.items():
            # Jahr extrahieren
            try:
                jahr = int(year_key.split("-")[1].strip())
            except:
                continue
            if jahr < jahr_range[0] or jahr > jahr_range[1]:
                continue
            if magazin_name.lower() == "spiegel":
                # SPIEGEL-Struktur
                for ausgabe in year_val.values():
                    for artikel in ausgabe.get("article", {}).values():
                        if not isinstance(artikel, dict): continue
                        # Rubrik
                        rubrik = artikel.get("category", [])
                        if isinstance(rubrik, list):
                            rubrik = ", ".join(rubrik) if rubrik else "keine Rubrik"
                        elif not rubrik:
                            rubrik = "keine Rubrik"
                        word_count = artikel.get("word_count")
                        try: word_count = int(word_count)
                        except: continue
                        rubrik_jahr[(rubrik, jahr)].append(word_count)
                        rubrik_counts[rubrik] += 1
            else:
                # STERN-Struktur
                for monat in year_val.values():
                    for eintrag in monat.values():
                        artikel_dict = eintrag.get("article", {}) if isinstance(eintrag, dict) else {}
                        for artikel in artikel_dict.values():
                            if not isinstance(artikel, dict): continue
                            rubrik = artikel.get("category", []) or artikel.get("article_category", [])
                            if isinstance(rubrik, list):
                                rubrik = ", ".join(rubrik) if rubrik else "keine Rubrik"
                            elif not rubrik:
                                rubrik = "keine Rubrik"
                            word_count = artikel.get("word_count")
                            try: word_count = int(word_count)
                            except: continue
                            rubrik_jahr[(rubrik, jahr)].append(word_count)
                            rubrik_counts[rubrik] += 1
    # In DataFrame bringen
    records = []
    for (rubrik, jahr), werte in rubrik_jahr.items():
        if len(werte) >= 1:
            records.append({
                "Rubrik": rubrik,
                "Jahr": jahr,
                "Ø Wörter": sum(werte)/len(werte),
                "Anzahl": len(werte)
            })
    df = pd.DataFrame(records)
    # Nur Rubriken mit insgesamt genug Artikeln
    top_rubriken = [
        r for r, c in sorted(rubrik_counts.items(), key=lambda x: -x[1]) if r != "keine Rubrik" and c >= min_anzahl
    ][:6]  # Top 6 
    return df, top_rubriken

# Spiegel
spiegel_dir = r"C:\Users\Matze\Notebooks\endres-webscraping\data\spiegel_json_data_nach_jahren"
df_spiegel, rubriken_spiegel = artikel_stats_pro_jahr_und_rubrik(
    spiegel_dir, "spiegel", jahr_range=(1947,2009), min_anzahl=25
)


# CSV aus vorherigem Schritt einlesen, falls nötig:
# jahreswerte = pd.read_csv(r".../1_artikel_laenge_stats_jahreswerte.csv", sep=";")

# Annahme: DataFrame heißt jahreswerte, mit Spalten:
# "jahr", "SPIEGEL_avg_words", "STERN_avg_words", "SPIEGEL_anzahl", "STERN_anzahl"
# SPIEGEL_anzahl = alle Artikel eines Jahres (alle Ausgaben aufsummiert)
# STERN_anzahl = alle Artikel eines Jahres (alle Monate aufsummiert)

# 1. Für den SPIEGEL: durchschnittliche Artikel pro Ausgabe berechnen (Anzahl Ausgaben pro Jahr = Zahl einzigartiger Monate)
jahreswerte["SPIEGEL_ausgaben"] = 52  # Standard-Wert, kann aber Jahr für Jahr abweichen!
# Wenn du das EXAKT ausrechnen willst: Berechne beim ursprünglichen Parsing die Zahl der Ausgaben pro Jahr und ergänze sie als Spalte.

# 2. Für den STERN: durch. Artikel pro Monat berechnen (12 Monate pro Jahr, außer Jahr ist nicht komplett)
jahreswerte["STERN_monate"] = 12  # wie oben, ggf. präzisieren

# Berechne die Durchschnitte:
jahreswerte["SPIEGEL_avg_artikel_pro_ausgabe"] = jahreswerte["SPIEGEL_anzahl"] / jahreswerte["SPIEGEL_ausgaben"]
jahreswerte["STERN_avg_artikel_pro_monat"]     = jahreswerte["STERN_anzahl"] / jahreswerte["STERN_monate"]

# PLOT

fig, ax1 = plt.subplots(figsize=(14,7))

# Farben
farbe_spiegel = "deepskyblue"
farbe_stern = "darkorange"
farbe_spiegel_artikel = "lightskyblue"
farbe_stern_artikel = "gold"

# Plot durch. Wörter pro Artikel
l1 = ax1.plot(jahreswerte["jahr"], jahreswerte["SPIEGEL_avg_words"], marker="o", color=farbe_spiegel, label="SPIEGEL: Ø Wörter/Artikel")
l2 = ax1.plot(jahreswerte["jahr"], jahreswerte["STERN_avg_words"], marker="s", color=farbe_stern, label="STERN: Ø Wörter/Artikel")
ax1.set_ylabel("Ø Wörter pro Artikel")
ax1.set_xlabel("Jahr")

# 2. Y-Achse für Artikelanzahl
ax2 = ax1.twinx()
l3 = ax2.plot(jahreswerte["jahr"], jahreswerte["SPIEGEL_avg_artikel_pro_ausgabe"], marker="o", color=farbe_spiegel_artikel, linestyle="--", label="SPIEGEL: Ø Artikel/Ausgabe")
l4 = ax2.plot(jahreswerte["jahr"], jahreswerte["STERN_avg_artikel_pro_monat"], marker="s", color=farbe_stern_artikel, linestyle="--", label="STERN: Ø Artikel/Monat")
ax2.set_ylabel("Ø Artikel pro Ausgabe/Monat")

# Legende zusammenführen
lines = l1 + l2 + l3 + l4
labels = [line.get_label() for line in lines]
ax1.legend(lines, labels, loc="upper left")

plt.title("Textlänge und Artikelanzahl pro Jahr (SPIEGEL und STERN)")
plt.tight_layout()
plt.savefig(r"C:\Users\Matze\Notebooks\endres-webscraping\data\research\1_artikel_laenge_words_jahreswert_mit_artikelanzahl.png")
plt.close()
print("Plot gespeichert: 1_artikel_laenge_words_jahreswert_mit_artikelanzahl.png")


plt.figure(figsize=(16,8))
for rubrik in rubriken_spiegel:
    d = df_spiegel[df_spiegel["Rubrik"]==rubrik].sort_values("Jahr")
    plt.plot(d["Jahr"], d["Ø Wörter"], marker="o", label=rubrik)
plt.title("Durchschnittliche Artikellänge nach Rubrik und Jahr (SPIEGEL)")
plt.xlabel("Jahr")
plt.ylabel("Ø Wörter pro Artikel")
plt.legend()
plt.tight_layout()
plt.savefig(os.path.join(output_dir, "1_spiegel_rubriken_jahresentwicklung.png"))
plt.close()
print("Plot gespeichert: 1_spiegel_rubriken_jahresentwicklung.png")

# Stern
stern_dir = r"C:\Users\Matze\Notebooks\endres-webscraping\data\stern_json_data_nach_jahren"
df_stern, rubriken_stern = artikel_stats_pro_jahr_und_rubrik(
    stern_dir, "stern", jahr_range=(2000,2025), min_anzahl=25
)

plt.figure(figsize=(16,8))
for rubrik in rubriken_stern:
    d = df_stern[df_stern["Rubrik"]==rubrik].sort_values("Jahr")
    plt.plot(d["Jahr"], d["Ø Wörter"], marker="o", label=rubrik)
plt.title("Durchschnittliche Artikellänge nach Rubrik und Jahr (STERN)")
plt.xlabel("Jahr")
plt.ylabel("Ø Wörter pro Artikel")
plt.legend()
plt.tight_layout()
plt.savefig(os.path.join(output_dir, "1_stern_rubriken_jahresentwicklung.png"))
plt.close()
print("Plot gespeichert: 1_stern_rubriken_jahresentwicklung.png")

# Plotten, falls gewünscht:
import matplotlib.pyplot as plt
plt.figure(figsize=(14,7))
plt.plot(jahreswerte["jahr"], jahreswerte["SPIEGEL_avg_words"], marker="o", label="SPIEGEL: Ø Wörter/Jahr")
plt.plot(jahreswerte["jahr"], jahreswerte["STERN_avg_words"], marker="s", label="STERN: Ø Wörter/Jahr")
plt.legend()
plt.title("Durchschnittliche Artikellänge (Wörter) pro Jahr")
plt.xlabel("Jahr")
plt.ylabel("Ø Wörter pro Artikel")
plt.tight_layout()
plt.savefig(r"C:\Users\Matze\Notebooks\endres-webscraping\data\research\1_artikel_laenge_words_jahreswert.png")
plt.close()
print("Jahres-Plot gespeichert!")

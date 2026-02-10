import os
import json
import re
import pandas as pd
from collections import defaultdict
from datetime import datetime
import matplotlib.pyplot as plt

#  KONFIGURATION 
spiegel_dir = r"C:\Users\Matze\Notebooks\endres-webscraping\data\spiegel_json_data_nach_jahren"
stern_dir   = r"C:\Users\Matze\Notebooks\endres-webscraping\data\stern_json_data_nach_jahren"
zieljahre   = [str(j) for j in range(2001, 2010)]
output_csv  = r"C:\Users\Matze\Notebooks\endres-webscraping\data\research\verantwortung_kategorien_dominanz.csv"
output_plot = r"C:\Users\Matze\Notebooks\endres-webscraping\data\research\verantwortung_kategorien_dominanz_plot.png"

#  Schlagwortgruppen 
spezifisch_911 = [
    r"\b9/11\b", r"\b9-11\b", r"\b11\. september\b", r"\belfter september\b", r"\btwin towers\b", r"\bworld trade center\b"
]
akteure = [r"\b(bin laden|osama)\b", r"\bal-qaida\b", r"\btaliban\b", r"\bterroristen?\b"]
abstrakt = [r"\bterror\b", r"\bterroranschlag\b", r"\banschlag\b", r"\battentat\b", r"\bgewalt\b", r"\bextremismus\b", r"\bislamismus\b"]

def normalisiere(text):
    subs = [
        (r"\bosama bin laden\b", "bin laden"),
        (r"\bobl\b", "bin laden"),
        (r"\bal quaida\b", "al-qaida"),
        (r"\bwtc\b", "world trade center"),
        (r"\btwin towers\b", "world trade center"),
        (r"\b11\. september\b", "9/11"),
        (r"\belfter september\b", "9/11"),
        (r"\bnine-eleven\b", "9/11"),
        (r"\b9-11\b", "9/11"),
    ]
    for pat, sub in subs:
        text = re.sub(pat, sub, text, flags=re.IGNORECASE)
    return text

zaehlungen = defaultdict(lambda: {"SPIEGEL_akteur":0, "SPIEGEL_abstrakt":0, "STERN_akteur":0, "STERN_abstrakt":0})

def kategorisiere_artikel(jahr, monat, quelle, saetze):
    akteur_count = 0
    abstrakt_count = 0
    found_911 = False
    for satz in saetze:
        satz_norm = normalisiere(satz.lower())
        if not any(re.search(pat, satz_norm) for pat in spezifisch_911):
            continue
        found_911 = True
        if any(re.search(pat, satz_norm) for pat in akteure):
            akteur_count += 1
        if any(re.search(pat, satz_norm) for pat in abstrakt):
            abstrakt_count += 1
    if found_911:
        if akteur_count > abstrakt_count:
            zaehlungen[f"{jahr}-{monat:02d}"][f"{quelle}_akteur"] += 1
        elif abstrakt_count > akteur_count:
            zaehlungen[f"{jahr}-{monat:02d}"][f"{quelle}_abstrakt"] += 1
        # Bei Gleichstand oder keine Nennung – kein Zähler

def process_jsons():
    # SPIEGEL
    for fname in os.listdir(spiegel_dir):
        if not fname.endswith(".json") or not fname.startswith("spiegel-"):
            continue
        jahr = fname[8:-5]
        if jahr not in zieljahre:
            continue
        with open(os.path.join(spiegel_dir, fname), encoding="utf-8") as f:
            try: data = json.load(f)
            except: continue
        ausgaben = data.get(f"Der Spiegel - {jahr}", {})
        for ausgabe in ausgaben.values():
            for artikel in ausgabe.get("article", {}).values():
                if not isinstance(artikel, dict): continue
                text = " ".join([
                    artikel.get("article_title") or "",
                    artikel.get("article_subtitle") or "",
                    artikel.get("article_text") or ""
                ])
                if not text.strip(): continue
                dat = artikel.get("article_publication_date","")
                try:
                    dt = datetime.fromisoformat(dat.split("+")[0])
                    j, m = dt.year, dt.month
                except: 
                    j, m = int(jahr), 1
                saetze = re.split(r"(?<=[.!?])\s+", text.strip())
                kategorisiere_artikel(j, m, "SPIEGEL", saetze)
    # STERN
    for fname in os.listdir(stern_dir):
        if not fname.endswith(".json") or not fname.startswith("stern-"):
            continue
        jahr = fname[6:-5]
        if jahr not in zieljahre:
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
            for eintrag in eintraege.values():
                artikel_dict = eintrag.get("article", {}) if isinstance(eintrag, dict) else {}
                for artikel in artikel_dict.values():
                    if not isinstance(artikel, dict): continue
                    text = " ".join([
                        artikel.get("article_title") or "",
                        artikel.get("article_subtitle") or "",
                        artikel.get("article_text") or ""
                    ])
                    if not text.strip(): continue
                    saetze = re.split(r"(?<=[.!?])\s+", text.strip())
                    kategorisiere_artikel(jahr, m, "STERN", saetze)

import matplotlib.pyplot as plt

def prozente_gesamt(df, output_csv, output_plot):
    """
    Berechnet und plottet die prozentuale Verteilung der dominanten Zuschreibungen
    (Akteur, Abstrakt, keine) pro Monat, für SPIEGEL+STERN gemeinsam.
    """
    df = df.copy()
    # Summiere pro Monat Akteur und Abstrakt für beide Medien
    df['Akteur_total'] = df['SPIEGEL_akteur'] + df['STERN_akteur']
    df['Abstrakt_total'] = df['SPIEGEL_abstrakt'] + df['STERN_abstrakt']
    df['gesamt'] = df['Akteur_total'] + df['Abstrakt_total']
    df['Akteur_prozent'] = df['Akteur_total'] / df['gesamt'] * 100
    df['Abstrakt_prozent'] = df['Abstrakt_total'] / df['gesamt'] * 100
    df['Neutral_prozent'] = 100 - (df['Akteur_prozent'] + df['Abstrakt_prozent'])
    
    # CSV speichern
    ausgabe = df[['monat', 'Akteur_prozent', 'Abstrakt_prozent', 'Neutral_prozent']].copy()
    ausgabe.to_csv(output_csv, sep=";", index=False, encoding="utf-8-sig")
    print(f"Prozentuale CSV (gesamt) gespeichert: {output_csv}")

    # Plot
    plt.figure(figsize=(16,7))
    plt.plot(df["monat_dt"], df["Akteur_prozent"], marker="o", label="Akteur-dominiert (%)")
    plt.plot(df["monat_dt"], df["Abstrakt_prozent"], marker="s", label="Abstrakt-dominiert (%)")
    plt.plot(df["monat_dt"], df["Neutral_prozent"], marker="x", label="Sonstige/Neutral (%)")
    plt.legend()
    plt.title("Dominante Zuschreibung in 9/11-Artikeln (% pro Monat, SPIEGEL+STERN gemeinsam)")
    plt.xlabel("Monat")
    plt.ylabel("Anteil der Artikel (%)")
    plt.tight_layout()
    plt.savefig(output_plot)
    plt.close()
    print(f"Prozent-Plot (gesamt) gespeichert: {output_plot}")

def prozente_getrennt(df, output_csv, output_plot):
    """
    Berechnet und plottet die prozentuale Verteilung getrennt für SPIEGEL und STERN.
    """
    df = df.copy()
    for quelle in ['SPIEGEL', 'STERN']:
        akteur = f"{quelle}_akteur"
        abstrakt = f"{quelle}_abstrakt"
        gesamt = f"{quelle}_gesamt"
        df[gesamt] = df[akteur] + df[abstrakt]
        df[f"{quelle}_Akteur_prozent"] = df[akteur] / df[gesamt] * 100
        df[f"{quelle}_Abstrakt_prozent"] = df[abstrakt] / df[gesamt] * 100
        df[f"{quelle}_Neutral_prozent"] = 100 - (df[f"{quelle}_Akteur_prozent"] + df[f"{quelle}_Abstrakt_prozent"])
    # CSV speichern
    cols = [
        'monat',
        'SPIEGEL_Akteur_prozent', 'SPIEGEL_Abstrakt_prozent', 'SPIEGEL_Neutral_prozent',
        'STERN_Akteur_prozent', 'STERN_Abstrakt_prozent', 'STERN_Neutral_prozent'
    ]
    ausgabe = df[cols].copy()
    ausgabe.to_csv(output_csv, sep=";", index=False, encoding="utf-8-sig")
    print(f"Prozentuale CSV (getrennt) gespeichert: {output_csv}")

    # Plot
    plt.figure(figsize=(16,7))
    plt.plot(df["monat_dt"], df["SPIEGEL_Akteur_prozent"], marker="o", label="SPIEGEL: Akteur-dominiert (%)")
    plt.plot(df["monat_dt"], df["SPIEGEL_Abstrakt_prozent"], marker="s", label="SPIEGEL: Abstrakt-dominiert (%)")
    plt.plot(df["monat_dt"], df["STERN_Akteur_prozent"], marker="^", label="STERN: Akteur-dominiert (%)")
    plt.plot(df["monat_dt"], df["STERN_Abstrakt_prozent"], marker="d", label="STERN: Abstrakt-dominiert (%)")
    plt.legend()
    plt.title("Dominante Zuschreibung in 9/11-Artikeln (% pro Monat, getrennt)")
    plt.xlabel("Monat")
    plt.ylabel("Anteil der Artikel (%)")
    plt.tight_layout()
    plt.savefig(output_plot)
    plt.close()
    print(f"Prozent-Plot (getrennt) gespeichert: {output_plot}")


#  Ausführen & Speichern 
process_jsons()
df = pd.DataFrame([
    {"monat": k, **v}
    for k,v in sorted(zaehlungen.items())
])
os.makedirs(os.path.dirname(output_csv), exist_ok=True)
df.to_csv(output_csv, sep=";", index=False, encoding="utf-8-sig")
print("CSV gespeichert:", output_csv)

#  Visualisierung 
df["monat_dt"] = pd.to_datetime(df["monat"], format="%Y-%m")
plt.figure(figsize=(16,7))
plt.plot(df["monat_dt"], df["SPIEGEL_akteur"], marker="o", label="SPIEGEL: Akteur-dominiert")
plt.plot(df["monat_dt"], df["SPIEGEL_abstrakt"], marker="s", label="SPIEGEL: Abstrakt-dominiert")
plt.plot(df["monat_dt"], df["STERN_akteur"], marker="^", label="STERN: Akteur-dominiert")
plt.plot(df["monat_dt"], df["STERN_abstrakt"], marker="d", label="STERN: Abstrakt-dominiert")
plt.legend()
plt.title("Mehrheitszuschreibung in 9/11-Artikeln: Akteur- vs. Abstraktbegriffe (pro Monat)")
plt.xlabel("Monat")
plt.ylabel("Anzahl Artikel (dominante Kategorie)")
plt.tight_layout()
plt.savefig(output_plot)
plt.close()

prozente_gesamt(
    df, 
    output_csv = r"C:\Users\Matze\Notebooks\endres-webscraping\data\research\verantwortung_prozent_gesamt.csv",
    output_plot = r"C:\Users\Matze\Notebooks\endres-webscraping\data\research\verantwortung_prozent_gesamt.png"
)

prozente_getrennt(
    df,
    output_csv = r"C:\Users\Matze\Notebooks\endres-webscraping\data\research\verantwortung_prozent_getrennt.csv",
    output_plot = r"C:\Users\Matze\Notebooks\endres-webscraping\data\research\verantwortung_prozent_getrennt.png"
)

print("Plot gespeichert:", output_plot)
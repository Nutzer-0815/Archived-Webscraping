import os
import json
from collections import defaultdict, Counter
import csv
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
import re

#  Konfiguration 
zieljahre = [str(j) for j in range(2001, 2010)]
spiegel_dir = r"C:\Users\Matze\Notebooks\endres-webscraping\data\spiegel_json_data_nach_jahren"
stern_dir   = r"C:\Users\Matze\Notebooks\endres-webscraping\data\stern_json_data_nach_jahren"
output_csv_abs = r"C:\Users\Matze\Notebooks\endres-webscraping\data\research\2_analyse_erweitert.csv"
output_csv_prozent = r"C:\Users\Matze\Notebooks\endres-webscraping\data\research\2_analyse_prozent.csv"

spezifische_schlagwoerter = {
    "wtc", "world trade center", "twin towers", "osama bin laden",
    "al-qaida", "al quaida", "bin laden", "nineeleven", "nine-eleven", "9/11", "9-11",
    "11. september", "elfter september"
}
spezifisch = [re.compile(rf"\b{re.escape(w)}\b", flags=re.IGNORECASE) for w in spezifische_schlagwoerter]
spezifisch_regex_map = {regex: word for regex, word in zip(spezifisch, spezifische_schlagwoerter)}

allgemeine_schlagwoerter = {
    "anschlag", "terrorakt", "attacke", "angriff", "attentat", "aktion", "tat", "überfall", "aggression"
}
allgemein = [re.compile(rf"\b{re.escape(w)}\b", flags=re.IGNORECASE) for w in allgemeine_schlagwoerter]
allgemein_regex_map = {regex: word for regex, word in zip(allgemein, allgemeine_schlagwoerter)}

def clean(text):
    return (text or "").strip().lower()

def analysiere_json():
    ergebnisse = defaultdict(lambda: {
        "anzahl": 0,
        "autoren": Counter(),
        "rubriken": Counter()
    })

    # Hilfsfunktion zum Verarbeiten eines Artikels
    def verarbeite_artikel(jahr, quelle, titel, subtitle, kicker, rubrik, autoren, text):
        alle_felder = " ".join([titel, subtitle, kicker, text])
        if any(sw.search(alle_felder) for sw in spezifisch):
            for ag in allgemein:
                if ag.search(alle_felder):
                    key = (jahr, quelle, allgemein_regex_map[ag])
                    ergebnisse[key]["anzahl"] += 1
                    for autor in autoren or ["keinAutor"]:
                        ergebnisse[key]["autoren"][autor.strip()] += 1
                    ergebnisse[key]["rubriken"][rubrik or "keineRubrik"] += 1

    # SPIEGEL
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
                titel = clean(artikel.get("article_title"))
                subtitle = clean(artikel.get("article_subtitle"))
                kicker = clean(" / ".join(artikel.get("article_kicker") or []))
                rubrik = clean(", ".join(artikel.get("category") or []))
                autoren = artikel.get("author") or []
                if isinstance(autoren, str):
                    autoren = [autoren]
                text = clean(artikel.get("article_text"))
                verarbeite_artikel(jahr, "SPIEGEL", titel, subtitle, kicker, rubrik, autoren, text)

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
        for daten in monate.values():  # Fix: direkt daten iterieren
            for eintrag in daten.values():
                if not isinstance(eintrag, dict):
                    continue
                artikel = eintrag.get("article", {})
                if not isinstance(artikel, dict):
                    continue
                for art in artikel.values():
                    if not isinstance(art, dict):
                        continue
                    titel = clean(art.get("article_title"))
                    subtitle = clean(art.get("article_subtitle"))
                    kicker = clean(" / ".join(art.get("article_kicker") or []))
                    rubrik = clean(", ".join(art.get("article_category") or []))
                    autoren = art.get("author") or []
                    if isinstance(autoren, str):
                        autoren = [autoren]
                    text = clean(art.get("article_text"))
                    verarbeite_artikel(jahr, "STERN", titel, subtitle, kicker, rubrik, autoren, text)
    return ergebnisse

def schreibe_csv_abs(ergebnisse: dict, pfad: str):
    os.makedirs(os.path.dirname(pfad), exist_ok=True)
    with open(pfad, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["jahr", "quelle", "schlagwort", "anzahl", "autor", "autor_anzahl", "rubrik", "rubrik_anzahl"])
        for (jahr, quelle, schlagwort), daten in sorted(ergebnisse.items()):
            writer.writerow([jahr, quelle, schlagwort, daten["anzahl"], "", "", "", ""])
            for autor, anzahl in daten["autoren"].items():
                writer.writerow(["", "", "", "", autor, anzahl, "", ""])
            for rubrik, anzahl in daten["rubriken"].items():
                writer.writerow(["", "", "", "", "", "", rubrik, anzahl])
    print(f" CSV gespeichert: {pfad}")

def schreibe_csv_prozent(ergebnisse: dict, pfad: str):
    # Aggregiere Summe pro (jahr, quelle)
    gruppensummen = defaultdict(int)
    for (jahr, quelle, _), daten in ergebnisse.items():
        gruppensummen[(jahr, quelle)] += daten["anzahl"]

    # Schreiben
    os.makedirs(os.path.dirname(pfad), exist_ok=True)
    with open(pfad, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["jahr", "quelle", "schlagwort", "anzahl", "prozent", "autor", "autor_anzahl", "rubrik", "rubrik_anzahl"])

        for (jahr, quelle, schlagwort), daten in sorted(ergebnisse.items()):
            gesamt = gruppensummen[(jahr, quelle)]
            prozent = (daten["anzahl"] / gesamt * 100) if gesamt > 0 else 0
            writer.writerow([jahr, quelle, schlagwort, daten["anzahl"], f"{prozent:.2f}", "", "", "", ""])
            
            for autor, anzahl in daten["autoren"].items():
                writer.writerow(["", "", "", "", "", autor, anzahl, "", ""])
            for rubrik, anzahl in daten["rubriken"].items():
                writer.writerow(["", "", "", "", "", "", "", rubrik, anzahl])

    print(f" Prozentuale CSV gespeichert: {pfad}")

#  Neue Visualisierungsfunktion 
def visualisiere_prozentuelle_daten(csv_path: str, output_dir: str):
    print("📊 Lade Daten für Visualisierung...")
    df = pd.read_csv(csv_path, delimiter=";", encoding="utf-8-sig")

    # Nur Zeilen mit Schlagwort-Zeile (also keine Autor- oder Rubrikzeilen)
    df_clean = df[df["schlagwort"].notna()].copy()

    # Korrigiere Datentyp
    # Stelle sicher, dass alle Werte als Strings interpretiert werden, bevor .str verwendet wird
    if df_clean["prozent"].dtype != "float":
        df_clean["prozent"] = df_clean["prozent"].astype(str).str.replace(",", ".").astype(float)


    # 1. Balkendiagramm
    plt.figure(figsize=(12, 6))
    sns.barplot(data=df_clean, x="jahr", y="prozent", hue="schlagwort")
    plt.title("Prozentuale Verteilung der Schlagwörter pro Jahr")
    plt.ylabel("Anteil (%)")
    plt.legend(title="Schlagwort", bbox_to_anchor=(1.05, 1), loc="upper left")
    plt.tight_layout()
    os.makedirs(output_dir, exist_ok=True)
    plt.savefig(os.path.join(output_dir, "2_balkendiagramm_prozent.png"))
    plt.close()
    print(" Balkendiagramm gespeichert.")

    # 2. Heatmap
    heatmap_df = df_clean.pivot_table(index="schlagwort", columns="jahr", values="prozent", aggfunc="sum").fillna(0)
    plt.figure(figsize=(12, 6))
    sns.heatmap(heatmap_df, annot=True, fmt=".1f", cmap="Reds", cbar_kws={'label': 'Anteil (%)'})
    plt.title("Heatmap: Schlagwortverteilung pro Jahr (prozentual)")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "2_heatmap_prozent.png"))
    plt.close()
    print(" Heatmap gespeichert.")

# Keyword-in-Context-Export  (CSV, Separator = ";")


def tokenize_w(text: str) -> list[str]:
    """
    Sehr einfache Tokenisierung (rein auf Wortebene, Buchstaben & Ziffern).
    """
    return re.findall(r"\b\w+\b", text.lower())

def kwic_export(ergebnisse: dict,
                output_path: str,
                zieljahre: list[str],
                allgemeine_set: set[str],
                spezifisch_set: set[str],
                spiegel_dir: str,
                stern_dir: str):
    """
    Erstellt eine CSV mit 3-Wörter-Kontext links/rechts & Artikel-URL.
    Spalten: jahr;quelle;keyword;kontext;url
    """
    #  1. Stelle fest, welche (jahr, quelle) & Schlagwörter überhaupt relevant sind
    relevante_keys = {(j,q,k) for (j,q,k) in ergebnisse.keys()}

    rows: list[list[str]] = []

    def add_kwic_row(jahr, quelle, keyword, tokens, pos, url):
        start = max(pos-3, 0)
        end   = min(pos+4, len(tokens))   # +4 weil slice end-exklusiv
        kontext = " ".join(tokens[start:pos] +
                           [f"**{tokens[pos]}**"] +   # Hervorhebung fürs Auge
                           tokens[pos+1:end])
        rows.append([jahr, quelle, keyword, kontext, url])

    #  2. Durchlaufe JSON-Korpus erneut und sammle KWIC
    for quelle, folder, file_prefix in [("SPIEGEL", spiegel_dir, "spiegel-"),
                                        ("STERN"  , stern_dir  , "stern-" )]:
        for fname in os.listdir(folder):
            if not fname.startswith(file_prefix) or not fname.endswith(".json"):
                continue
            jahr = fname[len(file_prefix):-5]          # "spiegel-" / "stern-" + ".json"
            if jahr not in zieljahre:
                continue
            with open(os.path.join(folder, fname), encoding="utf-8") as f:
                try:
                    data = json.load(f)
                except Exception:
                    continue

            if quelle == "SPIEGEL":
                ausgaben = data.get(f"Der Spiegel - {jahr}", {})
                for ausgabe in ausgaben.values():
                    for art in ausgabe.get("article", {}).values():
                        if not isinstance(art, dict):
                            continue
                        url  = art.get("article_url") or ""
                        text = (art.get("article_text") or "").lower()
                        titel = (art.get("article_title") or "").lower()
                        subtitle = (art.get("article_subtitle") or "").lower()
                        kicker = " / ".join(art.get("article_kicker") or []).lower()
                        full   = " ".join([titel, subtitle, kicker, text])

                        # Muss wenigstens EIN spezifisches und EIN allgemeines enthalten
                        if not any(sw.search(full) for sw in spezifisch_set):
                            continue
                        tokens = tokenize_w(full)
                        for i,tok in enumerate(tokens):
                            if tok in allgemeine_set and (jahr, quelle, tok) in relevante_keys:
                                add_kwic_row(jahr, quelle, tok, tokens, i, url)

            else:  # STERN
                monate = data.get(f"Stern - {jahr}", {})
                for monat_d in monate.values():
                    for eintrag in monat_d.values():
                        if not isinstance(eintrag, dict):
                            continue
                        for art in (eintrag.get("article", {}) or {}).values():
                            if not isinstance(art, dict):
                                continue
                            url  = art.get("article_url") or ""
                            text = (art.get("article_text") or "").lower()
                            titel = (art.get("article_title") or "").lower()
                            subtitle = (art.get("article_subtitle") or "").lower()
                            kicker = " / ".join(art.get("article_kicker") or []).lower()
                            full   = " ".join([titel, subtitle, kicker, text])

                            if not any(sw.search(full) for sw in spezifisch_set):
                                continue
                            tokens = tokenize_w(full)
                            for i,tok in enumerate(tokens):
                                if tok in allgemeine_set and (jahr, quelle, tok) in relevante_keys:
                                    add_kwic_row(jahr, quelle, tok, tokens, i, url)

    #  3. Schreibe CSV
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8-sig", newline="") as f_out:
        writer = csv.writer(f_out, delimiter=";")
        writer.writerow(["jahr", "quelle", "keyword", "kontext", "url"])
        writer.writerows(rows)

    print(f" KWIC-CSV gespeichert: {output_path}")


def extrahiere_schlagwoerter_im_satzkontext(ergebnisse, spiegel_dir, stern_dir, zieljahre, spezifisch, allgemein, output_path):
    rows = []

    def process_text_satzweise(jahr, quelle, artikel, spezifisch, allgemein, url):
        text = artikel.get("article_text", "") or ""
        saetze = re.split(r'(?<=[.!?])\s+', text.strip())

        for satz in saetze:
            satz_clean = satz.lower()
            if any(ag.search(satz_clean) for ag in allgemein) and any(sp.search(satz_clean) for sp in spezifisch):
                for ag_re in allgemein:
                    ag_text = allgemein_regex_map[ag_re]
                    if ag_re.search(satz_clean):
                        for sp_re in spezifisch:
                            sp_text = spezifisch_regex_map[sp_re]
                            if sp_re.search(satz_clean):
                                # KWIC erzeugen
                                tokens = re.findall(r'\b\w+\b', satz)
                                kwic = ""
                                for i, tok in enumerate(tokens):
                                    if tok.lower() == ag_text.lower():
                                        start = max(i - 3, 0)
                                        end = min(i + 4, len(tokens))
                                        kwic = " ".join(tokens[start:i] + [f"{tokens[i]}"] + tokens[i+1:end])
                                        break
                                rows.append([
                                    jahr, quelle, ag_text, sp_text,
                                    satz.strip(), kwic.strip(), url or ""
                                ])


    # SPIEGEL
    for fname in os.listdir(spiegel_dir):
        if not fname.endswith(".json") or not fname.startswith("spiegel-"):
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
                url = artikel.get("article_url") or artikel.get("url") or ""
                process_text_satzweise(jahr, "SPIEGEL", artikel, spezifisch, allgemein, url)

    # STERN
    for fname in os.listdir(stern_dir):
        if not fname.endswith(".json") or not fname.startswith("stern-"):
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
        for monat in monate.values():
            for eintrag in monat.values():
                if not isinstance(eintrag, dict):
                    continue
                artikel_dict = eintrag.get("article", {})
                if not isinstance(artikel_dict, dict):
                    continue
                for art in artikel_dict.values():
                    if not isinstance(art, dict):
                        continue
                    url = art.get("url", "")
                    process_text_satzweise(jahr, "STERN", art, spezifisch, allgemein, url)


    # Schreiben
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["jahr", "quelle", "allgemeines_schlagwort", "spezifisches_schlagwort", "satz", "kwic", "url"])
        writer.writerows(rows)

    print(f" Kontext-CSV gespeichert: {output_path}")
def visualisiere_satzkontext_csv(csv_path: str, output_dir: str):
    df = pd.read_csv(csv_path, delimiter=";", encoding="utf-8-sig")

    # ABSOLUTE WERTE

    print("Erstelle absolute Balkendiagramme und Heatmaps...")

    plt.figure(figsize=(12, 6))
    df_balken = df.groupby(["jahr", "allgemeines_schlagwort"]).size().reset_index(name="anzahl")
    sns.barplot(data=df_balken, x="jahr", y="anzahl", hue="allgemeines_schlagwort")
    plt.title("Absolute Häufigkeit: Allgemeine Schlagwörter mit spezifischen pro Jahr")
    plt.ylabel("Vorkommen")
    plt.legend(title="Allgemeines Schlagwort", bbox_to_anchor=(1.05, 1), loc="upper left")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "2_balkendiagramm_satzkontext_absolut.png"))
    plt.close()

    df_heat = df.groupby(["allgemeines_schlagwort", "jahr", "quelle"]).size().reset_index(name="anzahl")
    df_pivot = df_heat.pivot_table(index="allgemeines_schlagwort", columns=["jahr", "quelle"], values="anzahl", fill_value=0)

    plt.figure(figsize=(14, 8))
    sns.heatmap(df_pivot, annot=True, fmt=".0f", cmap="YlGnBu", cbar_kws={"label": "Vorkommen"})
    plt.title("Heatmap (absolut): Allgemeine Schlagwörter mit spezifischen im Satz")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "2_heatmap_satzkontext_absolut.png"))
    plt.close()

    print(" Absolute Diagramme gespeichert.")


    # PROZENTUALE WERTE

    print("Erstelle prozentuale Balkendiagramme und Heatmaps...")

    # Berechne Gesamtanzahl pro Jahr/Quelle für Normalisierung
    gesamt = df.groupby(["jahr", "quelle"]).size().reset_index(name="gesamt")
    df_merged = df.merge(gesamt, on=["jahr", "quelle"])
    df_merged["prozent"] = 100 / df_merged["gesamt"]

    df_prozent = df_merged.groupby(["jahr", "allgemeines_schlagwort"])["prozent"].sum().reset_index()

    plt.figure(figsize=(12, 6))
    sns.barplot(data=df_prozent, x="jahr", y="prozent", hue="allgemeines_schlagwort")
    plt.title("Prozentual: Allgemeine Schlagwörter mit spezifischen pro Jahr")
    plt.ylabel("Anteil (%)")
    plt.legend(title="Allgemeines Schlagwort", bbox_to_anchor=(1.05, 1), loc="upper left")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "2_balkendiagramm_satzkontext_prozent.png"))
    plt.close()

    df_heat_proz = df_merged.groupby(["allgemeines_schlagwort", "jahr", "quelle"])["prozent"].sum().reset_index()
    df_pivot_proz = df_heat_proz.pivot_table(index="allgemeines_schlagwort", columns=["jahr", "quelle"], values="prozent", fill_value=0)

    plt.figure(figsize=(14, 8))
    sns.heatmap(df_pivot_proz, annot=True, fmt=".1f", cmap="YlOrBr", cbar_kws={"label": "Anteil (%)"})
    plt.title("Heatmap (prozentual): Allgemeine Schlagwörter mit spezifischen im Satz")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "2_heatmap_satzkontext_prozent.png"))
    plt.close()

    print(" Prozentuale Diagramme gespeichert.")


#  MAIN 
if __name__ == "__main__":
    kwic_csv = r"C:\Users\Matze\Notebooks\endres-webscraping\data\research\2_kwic_auszug.csv"
    satzkontext_csv = r"C:\Users\Matze\Notebooks\endres-webscraping\data\research\2_kontext_schlagwoerter.csv"
    

    print("Lade JSON-Daten...")
    ergebnisse = analysiere_json()
    print("Speichere absolute Zahlen...")
    schreibe_csv_abs(ergebnisse, output_csv_abs)
    print("Berechne Prozentwerte...")
    schreibe_csv_prozent(ergebnisse, output_csv_prozent)
    print("Erstelle Visualisierungen...")
    visualisiere_prozentuelle_daten(output_csv_prozent, os.path.dirname(output_csv_prozent))
    kwic_export(ergebnisse,kwic_csv,zieljahre,allgemein,spezifisch,spiegel_dir,stern_dir)
    print("Extrahiere Satz-Kontext...")
    extrahiere_schlagwoerter_im_satzkontext(ergebnisse, spiegel_dir, stern_dir, zieljahre,spezifisch, allgemein,r"C:\Users\Matze\Notebooks\endres-webscraping\data\research\2_kontext_schlagwoerter.csv")
    visualisiere_satzkontext_csv(satzkontext_csv, os.path.dirname(satzkontext_csv))

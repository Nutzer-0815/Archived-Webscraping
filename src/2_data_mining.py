import os
import json
from collections import defaultdict

# Verzeichnisse
json_dir = r"C:\Users\Matze\Notebooks\endres-webscraping\data\research"
output_count_path = os.path.join(json_dir, "0_ausgabeanzahl_pro_jahr.json")
output_list_path = os.path.join(json_dir, "0_ausgaben_pro_jahr.json")
output_sorted_list_path = os.path.join(json_dir, "0_ausgaben_pro_jahr_sortiert.json")
output_missing_path = os.path.join(json_dir, "0_fehlende_ausgaben_pro_jahr.json")
output_article_count_path = os.path.join(json_dir, "0_artikelanzahl_pro_ausgabe.json")
output_missing_articles_path = os.path.join(json_dir, "0_fehlende_artikelnummern.json")
output_empty_articles_path = os.path.join(json_dir, "0_leere_artikel.json")
output_null_articles_path = os.path.join(json_dir, "0_artikel_mit_nullnummer.json")

# 1
issue_counts_per_year = {}
issue_numbers_per_year = defaultdict(list)

for filename in sorted(os.listdir(json_dir)):
    if filename.endswith(".json") and not filename.startswith("0_"):
        filepath = os.path.join(json_dir, filename)
        with open(filepath, "r", encoding="utf-8") as file:
            try:
                data = json.load(file)
            except json.JSONDecodeError as e:
                print(f"Fehler beim Laden von {filename}: {e}")
                continue

            for year_key, issues in data.items():
                year = year_key.split(" - ")[-1]
                for issue_id, issue_data in issues.items():
                    if isinstance(issue_data, dict) and 'issue_number' in issue_data:
                        issue_number = issue_data['issue_number']
                        issue_numbers_per_year[year].append(issue_number)

for year, issues in issue_numbers_per_year.items():
    issue_counts_per_year[year] = len(issues)

with open(output_count_path, "w", encoding="utf-8") as f:
    json.dump(issue_counts_per_year, f, ensure_ascii=False, indent=2)

with open(output_list_path, "w", encoding="utf-8") as f:
    json.dump(issue_numbers_per_year, f, ensure_ascii=False, indent=2)

# 2
sorted_issue_numbers_per_year = {
    year: sorted(issue_numbers_per_year[year])
    for year in issue_numbers_per_year
}

with open(output_sorted_list_path, "w", encoding="utf-8") as f:
    json.dump(sorted_issue_numbers_per_year, f, ensure_ascii=False, indent=2)

# 3
fehlende_ausgaben = defaultdict(list)

for year in sorted(issue_numbers_per_year):
    monat_liste = sorted(
        int(issue.split("-")[1]) for issue in issue_numbers_per_year[year]
        if "-" in issue and issue.split("-")[0] == year
    )

    if not monat_liste:
        continue

    min_m = min(monat_liste)
    max_m = max(monat_liste)
    vorhandene = set(monat_liste)
    fehlende = [f"{year}-{str(m).zfill(2)}" for m in range(min_m, max_m + 1) if m not in vorhandene]

    if fehlende:
        fehlende_ausgaben[year].extend(fehlende)

with open(output_missing_path, "w", encoding="utf-8") as f:
    json.dump(fehlende_ausgaben, f, ensure_ascii=False, indent=2)

# 4
artikelanzahl_pro_ausgabe = {}

for year in sorted(os.listdir(json_dir)):
    if not year.endswith(".json") or year.startswith("0_"):
        continue
    filepath = os.path.join(json_dir, year)

    with open(filepath, "r", encoding="utf-8") as file:
        try:
            data = json.load(file)
        except json.JSONDecodeError:
            continue

        for _, ausgaben in data.items():
            for ausgabe_id, ausgabe_data in ausgaben.items():
                if not isinstance(ausgabe_data, dict):
                    continue

                artikel_dict = ausgabe_data.get("article", {})
                artikelanzahl = sum(
                    1 for key in artikel_dict if key.startswith("article - ")
                )

                artikelanzahl_pro_ausgabe[ausgabe_id] = artikelanzahl

with open(output_article_count_path, "w", encoding="utf-8") as f:
    json.dump(artikelanzahl_pro_ausgabe, f, ensure_ascii=False, indent=2)

# 5
fehlende_artikel = defaultdict(lambda: defaultdict(list))
leere_artikel = defaultdict(lambda: defaultdict(list))
nullnummern_artikel = defaultdict(lambda: defaultdict(list))  # NEU

for filename in sorted(os.listdir(json_dir)):
    if not filename.endswith(".json") or filename.startswith("0_"):
        continue

    filepath = os.path.join(json_dir, filename)

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
                artikelnummern = []
                max_artikelnr = 0

                for key, artikel_data in artikel_dict.items():
                    if key.strip().lower() == "article - null":
                        nullnummern_artikel[jahr][ausgabe_id].append(artikel_data.get("article_title", "Unbekannt"))
                        continue

                    try:
                        if not key.startswith("article - "):
                            continue
                        nr = int(key.split("-")[1])
                        artikelnummern.append(nr)
                        max_artikelnr = max(max_artikelnr, nr)

                        if (
                            isinstance(artikel_data, dict)
                            and artikel_data.get("article_title") in [None, "", "null"]
                            and artikel_data.get("article_subtitle") in [None, "", "null"]
                        ):
                            leere_artikel[jahr][ausgabe_id].append(f"{str(nr).zfill(2)}")

                    except (IndexError, ValueError):
                        continue

                if artikelnummern:
                    artikelnummern.sort()
                    min_n = artikelnummern[0]
                    max_n = artikelnummern[-1]
                    vorhanden = set(artikelnummern)
                    fehlende = [
                        f"{str(n).zfill(2)} / {str(max_n).zfill(2)}"
                        for n in range(min_n, max_n + 1) if n not in vorhanden
                    ]
                    if fehlende:
                        fehlende_artikel[jahr][ausgabe_id] = fehlende

with open(output_missing_articles_path, "w", encoding="utf-8") as f:
    json.dump(fehlende_artikel, f, ensure_ascii=False, indent=2)

with open(output_empty_articles_path, "w", encoding="utf-8") as f:
    json.dump(leere_artikel, f, ensure_ascii=False, indent=2)

with open(output_null_articles_path, "w", encoding="utf-8") as f:  # NEU
    json.dump(nullnummern_artikel, f, ensure_ascii=False, indent=2)

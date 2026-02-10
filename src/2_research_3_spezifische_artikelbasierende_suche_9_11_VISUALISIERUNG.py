import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os

#  Konfiguration 
csv_path = r"C:\Users\Matze\Notebooks\endres-webscraping\data\research\analyse_erweitert.csv"
output_dir = r"C:\Users\Matze\Notebooks\endres-webscraping\data\research\plots"
os.makedirs(output_dir, exist_ok=True)

#  Daten laden 
df = pd.read_csv(csv_path, delimiter=";")

# Nur Zeilen mit Schlagwort und Anzahl
df = df[df["schlagwort"].notna() & df["anzahl"].notna()]
df = df[["jahr", "quelle", "schlagwort", "anzahl"]].copy()
df["anzahl"] = df["anzahl"].astype(int)

# Pivot für Heatmap
pivot = df.pivot_table(index="schlagwort", columns="jahr", values="anzahl", aggfunc="sum", fill_value=0)

#  Heatmap 
plt.figure(figsize=(12, 8))
sns.heatmap(pivot, annot=True, fmt="d", cmap="YlOrRd", linewidths=0.5, cbar_kws={"label": "Anzahl"})
plt.title("Häufigkeit allgemeiner Schlagwörter pro Jahr (in Artikeln mit 9/11-Bezug)")
plt.ylabel("Schlagwort")
plt.xlabel("Jahr")
plt.tight_layout()
heatmap_path = os.path.join(output_dir, "heatmap_schlagwoerter.png")
plt.savefig(heatmap_path)
plt.close()
print(f" Heatmap gespeichert: {heatmap_path}")

#  Balkendiagramme nach Schlagwort 
for schlagwort in df["schlagwort"].unique():
    data = df[df["schlagwort"] == schlagwort].groupby(["jahr", "quelle"])["anzahl"].sum().unstack().fillna(0)
    data.plot(kind="bar", figsize=(10, 6))
    plt.title(f"Häufigkeit von '{schlagwort}' pro Jahr und Quelle")
    plt.ylabel("Anzahl Artikel")
    plt.xlabel("Jahr")
    plt.xticks(rotation=45)
    plt.tight_layout()
    bar_path = os.path.join(output_dir, f"balkendiagramm_{schlagwort.replace(' ', '_')}.png")
    plt.savefig(bar_path)
    plt.close()
    print(f" Balkendiagramm für '{schlagwort}' gespeichert: {bar_path}")

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

#  Neue Visualisierungsfunktion 
def visualisiere_prozentuelle_daten(csv_path: str, output_dir: str):
    print("Lade Daten für Visualisierung...")
    df = pd.read_csv(csv_path, delimiter=";", encoding="utf-8-sig")

    # Nur Zeilen mit Schlagwort-Zeile (also keine Autor- oder Rubrikzeilen)
    df_clean = df[df["schlagwort"].notna()].copy()

    # Korrigiere Datentyp
    df_clean["prozent"] = df_clean["prozent"].str.replace(",", ".").astype(float)

    # 1. Balkendiagram
    plt.figure(figsize=(12, 6))
    sns.barplot(data=df_clean, x="jahr", y="prozent", hue="schlagwort")
    plt.title("Prozentuale Verteilung der Schlagwörter pro Jahr")
    plt.ylabel("Anteil (%)")
    plt.legend(title="Schlagwort", bbox_to_anchor=(1.05, 1), loc="upper left")
    plt.tight_layout()
    os.makedirs(output_dir, exist_ok=True)
    plt.savefig(os.path.join(output_dir, "balkendiagramm_prozent.png"))
    plt.close()
    print(" Balkendiagramm gespeichert.")

    # 2. Heatmap
    heatmap_df = df_clean.pivot_table(index="schlagwort", columns="jahr", values="prozent", aggfunc="sum").fillna(0)
    plt.figure(figsize=(12, 6))
    sns.heatmap(heatmap_df, annot=True, fmt=".1f", cmap="Reds", cbar_kws={'label': 'Anteil (%)'})
    plt.title("Heatmap: Schlagwortverteilung pro Jahr (prozentual)")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "heatmap_prozent.png"))
    plt.close()
    print(" Heatmap gespeichert.")

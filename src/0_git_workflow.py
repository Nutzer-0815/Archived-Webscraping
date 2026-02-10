"""
Git-Workflow zur Versionierung des Codes.
Teil der Codesammlung der Masterarbeit von Matthias Endres.
1.0 einfach durchlaufen lassen, um zu committen.
"""

# 1.0 

import os
from os import system
import subprocess

# Sollte eigentlich in src sein, wenn nicht, merkt man das schon sehr schnell.
print("Startverzeichnis:", os.getcwd())

# Nach oben springen, um wirklich alles zu versionieren.
os.chdir("../")
#! os.chdir("./endres-webscraping") # wenn ich mal wieder eins zu hoch gerutscht bin.
print("Verzeichnis nach Wechsel:", os.getcwd())



# Git ausführen.

result = subprocess.run(["git", "status"], capture_output=True, text=True)
print(result.stdout) 

# git add .
add = subprocess.run(["git", "add", "."], check=True)

# git commit -m "Deine Nachricht"
commit = subprocess.run(["git", "commit", "-m", "!!!! FERTIG !!! Nix mehr anfassen!!!"], check=True)

# git push origin new
push = subprocess.run(["git", "push", "origin", "new"], check=True)


"""
# ACHTUNG! Überschreibt alles.
# 2.0 - Wenn das git-Verzeichnis ins lokale Verzeichnis übertragen werden soll (Ohne Rücksicht auf Verluste).

# git fetch --all
fetch = subprocess.run(["git", "fetch", "--all"], check=True)
print(fetch.stdout)

# git reset --hard origin/new
reset_hard = subprocess.run(["git", "reset", "--hard", "origin/new"], check=True)
print(reset_hard.stdout)
"""
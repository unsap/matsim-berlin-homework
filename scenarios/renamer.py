#!/usr/bin/env python3

import os
import re

def rename(number, abbreviation, old_abbreviation = None):
    if old_abbreviation is None:
        old_abbreviation = abbreviation
    filename_re = re.compile(rf"(?:{number}\.)?(?:berlin-v5.5(?:-10pct)?(?:-{old_abbreviation}|-{abbreviation})?|{old_abbreviation}|{abbreviation})(.+?)(?:-{old_abbreviation}|-{abbreviation})?(\..+)")
    for dirpath, dirnames, filenames in os.walk(f"{number}.{abbreviation}"):
        for filename in filenames:
            match = filename_re.match(filename)
            if match:
                renamed = f"{number}.{abbreviation}{match.group(1)}{match.group(2)}"
                if filename != renamed:
                    print(f"Renaming {filename} to {renamed}")
                    os.rename(os.path.join(dirpath, filename), os.path.join(dirpath, renamed))

rename(0, "BASE")
rename(1, "GR-HS")
rename(2, "GR-WS")
rename(3, "KR-HS")
rename(4, "KB", "KB-2")
rename(5, "MV")
rename(6, "S1")
rename(7, "S2")
rename(8, "S3", "S3-2")

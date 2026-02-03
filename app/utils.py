from datetime import date
from typing import List, Tuple

def iso_year_week(d: date) -> Tuple[int, int]:
    iso = d.isocalendar()
    return iso[0], iso[1]

def extract_hashtags(text: str | None) -> List[str]:
    if not text: return []
    res, cur, inside = [], [], False
    for ch in text:
        if ch == '#':
            if inside and cur:
                res.append(''.join(cur).lower())
                cur = []
            inside = True
            continue
        if inside and (ch.isalnum() or ch == '_'):
            cur.append(ch)
        elif inside:
            if cur: res.append(''.join(cur).lower())
            cur, inside = [], False
    if inside and cur: res.append(''.join(cur).lower())
    # uniq keep order
    seen, out = set(), []
    for h in res:
        if h not in seen:
            seen.add(h); out.append(h)
    return out

def find_keywords(text: str | None, keywords: list[str]) -> list[str]:
    if not text: return []
    t = text.lower()
    found = []
    seen = set()
    for kw in keywords:
        k = kw.strip().lower()
        if not k: continue
        if k in seen: continue
        if k in t:
            found.append(k); seen.add(k)
    return found

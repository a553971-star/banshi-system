"""
bible_loader.py — 磐石決策系統
讀取本地 bible.json，提供每日聖經經文（和合本）。
"""

import hashlib
import json
import os
import random

_DIR = os.path.dirname(os.path.abspath(__file__))
_BIBLE_PATH = os.path.join(_DIR, "bible.json")
_BIBLE_URL = (
    "https://raw.githubusercontent.com/scrollmapper/"
    "bible_databases/master/formats/json/ChiUn.json"
)

_FALLBACK = ("你的話是我腳前的燈，是我路上的光。", "詩篇 119:105")

_ZH_NAMES: dict[str, str] = {
    "Genesis": "創世記",
    "Exodus": "出埃及記",
    "Leviticus": "利未記",
    "Numbers": "民數記",
    "Deuteronomy": "申命記",
    "Joshua": "約書亞記",
    "Judges": "士師記",
    "Ruth": "路得記",
    "1 Samuel": "撒母耳記上",
    "2 Samuel": "撒母耳記下",
    "1 Kings": "列王紀上",
    "2 Kings": "列王紀下",
    "1 Chronicles": "歷代志上",
    "2 Chronicles": "歷代志下",
    "Ezra": "以斯拉記",
    "Nehemiah": "尼希米記",
    "Esther": "以斯帖記",
    "Job": "約伯記",
    "Psalms": "詩篇",
    "Proverbs": "箴言",
    "Ecclesiastes": "傳道書",
    "Song of Solomon": "雅歌",
    "Song of Songs": "雅歌",
    "Isaiah": "以賽亞書",
    "Jeremiah": "耶利米書",
    "Lamentations": "耶利米哀歌",
    "Ezekiel": "以西結書",
    "Daniel": "但以理書",
    "Hosea": "何西阿書",
    "Joel": "約珥書",
    "Amos": "阿摩司書",
    "Obadiah": "俄巴底亞書",
    "Jonah": "約拿書",
    "Micah": "彌迦書",
    "Nahum": "那鴻書",
    "Habakkuk": "哈巴谷書",
    "Zephaniah": "西番雅書",
    "Haggai": "哈該書",
    "Zechariah": "撒迦利亞書",
    "Malachi": "瑪拉基書",
    "Matthew": "馬太福音",
    "Mark": "馬可福音",
    "Luke": "路加福音",
    "John": "約翰福音",
    "Acts": "使徒行傳",
    "Romans": "羅馬書",
    "1 Corinthians": "哥林多前書",
    "2 Corinthians": "哥林多後書",
    "Galatians": "加拉太書",
    "Ephesians": "以弗所書",
    "Philippians": "腓立比書",
    "Colossians": "歌羅西書",
    "1 Thessalonians": "帖撒羅尼迦前書",
    "2 Thessalonians": "帖撒羅尼迦後書",
    "1 Timothy": "提摩太前書",
    "2 Timothy": "提摩太後書",
    "Titus": "提多書",
    "Philemon": "腓利門書",
    "Hebrews": "希伯來書",
    "James": "雅各書",
    "1 Peter": "彼得前書",
    "2 Peter": "彼得後書",
    "1 John": "約翰一書",
    "2 John": "約翰二書",
    "3 John": "約翰三書",
    "Jude": "猶大書",
    "Revelation of John": "啟示錄",
    "Revelation": "啟示錄",
}


def ensure_bible_json(path: str = _BIBLE_PATH) -> bool:
    """若 bible.json 不存在則下載。回傳是否可用。"""
    if os.path.exists(path):
        return True
    try:
        import requests
        resp = requests.get(_BIBLE_URL, timeout=30)
        resp.raise_for_status()
        with open(path, "w", encoding="utf-8") as f:
            f.write(resp.text)
        return True
    except Exception:
        return False


def _load_verses(path: str = _BIBLE_PATH) -> list[tuple[str, int, int, str]]:
    """
    讀取 bible.json，回傳 [(book_name_en, chapter, verse, text), ...]。
    失敗回傳空清單。
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        verses = []
        for book in data.get("books", []):
            book_name = book.get("name", "")
            for ch in book.get("chapters", []):
                chap_num = ch.get("chapter", 0)
                for v in ch.get("verses", []):
                    text = v.get("text", "").strip()
                    verse_num = v.get("verse", 0)
                    if text:
                        verses.append((book_name, chap_num, verse_num, text))
        return verses
    except Exception:
        return []


def get_daily_verse(date_str: str) -> tuple[str, str]:
    """
    用 date_str 作為 seed 從 bible.json 選一則經文。
    回傳 (經文內容, 出處) tuple。失敗回傳 _FALLBACK。
    """
    if not ensure_bible_json():
        return _FALLBACK

    verses = _load_verses()
    if not verses:
        return _FALLBACK

    seed = int(hashlib.md5(date_str.encode()).hexdigest(), 16) % (2 ** 32)
    rng = random.Random(seed)
    book_en, chapter, verse_num, text = rng.choice(verses)

    book_zh = _ZH_NAMES.get(book_en, book_en)
    reference = f"{book_zh} {chapter}:{verse_num}（和合本）"
    return text, reference

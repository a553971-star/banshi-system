"""
notes_book_store.py — 研究筆記本持久化（GitHub API）
資料結構：
[
  {
    "id": "timestamp",
    "title": "等回檔",
    "stocks": [
      {
        "stock_id": "2330",
        "name": "台積電",
        "note": "等跌到 900 再進",
        "created_price": 945.0,
        "added_at": "2026-05-20"
      }
    ]
  }
]
"""
import json
import os
import urllib.request
import urllib.error

_REPO  = os.getenv("GH_REPO", "")
_TOKEN = os.getenv("GH_PAT", "")
_PATH  = "notes_book.json"
_API   = f"https://api.github.com/repos/{_REPO}/contents/{_PATH}"


def _gh_get() -> tuple:
    if not _REPO or not _TOKEN:
        return [], None
    try:
        req = urllib.request.Request(
            _API,
            headers={
                "Authorization": f"token {_TOKEN}",
                "Accept": "application/vnd.github.v3+json",
            }
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
        import base64
        content = json.loads(base64.b64decode(data["content"]).decode())
        return content, data["sha"]
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return [], None
        return [], None
    except Exception:
        return [], None


def _gh_put(content: list, sha: str = None) -> bool:
    if not _REPO or not _TOKEN:
        return False
    try:
        import base64
        body = {
            "message": "update notes_book",
            "content": base64.b64encode(
                json.dumps(content, ensure_ascii=False, indent=2).encode()
            ).decode(),
        }
        if sha:
            body["sha"] = sha
        data = json.dumps(body).encode()
        req = urllib.request.Request(
            _API,
            data=data,
            headers={
                "Authorization": f"token {_TOKEN}",
                "Accept": "application/vnd.github.v3+json",
                "Content-Type": "application/json",
            },
            method="PUT",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
        return True
    except Exception:
        return False


def load_notes_book() -> list:
    content, _ = _gh_get()
    return content


def save_notes_book(sections: list) -> bool:
    _, sha = _gh_get()
    return _gh_put(sections, sha)


def add_section(title: str) -> bool:
    import time
    sections = load_notes_book()
    sections.append({
        "id": str(int(time.time())),
        "title": title,
        "stocks": [],
    })
    return save_notes_book(sections)


def rename_section(section_id: str, new_title: str) -> bool:
    sections = load_notes_book()
    for s in sections:
        if s["id"] == section_id:
            s["title"] = new_title
            break
    return save_notes_book(sections)


def delete_section(section_id: str) -> bool:
    sections = load_notes_book()
    sections = [s for s in sections if s["id"] != section_id]
    return save_notes_book(sections)


def add_stock(
    section_id: str,
    stock_id: str,
    name: str,
    note: str = "",
    created_price=None,
) -> bool:
    import datetime
    sections = load_notes_book()
    for s in sections:
        if s["id"] == section_id:
            # 同一股票已存在就更新
            s["stocks"] = [x for x in s["stocks"] if x["stock_id"] != stock_id]
            s["stocks"].append({
                "stock_id":     stock_id,
                "name":         name,
                "note":         note,
                "created_price": created_price,  # 加入時的當下價格，供日後回顧
                "added_at":     str(datetime.date.today()),
            })
            break
    return save_notes_book(sections)


def update_note(section_id: str, stock_id: str, note: str) -> bool:
    sections = load_notes_book()
    for s in sections:
        if s["id"] == section_id:
            for st in s["stocks"]:
                if st["stock_id"] == stock_id:
                    st["note"] = note
                    break
            break
    return save_notes_book(sections)


def remove_stock(section_id: str, stock_id: str) -> bool:
    sections = load_notes_book()
    for s in sections:
        if s["id"] == section_id:
            s["stocks"] = [x for x in s["stocks"] if x["stock_id"] != stock_id]
            break
    return save_notes_book(sections)

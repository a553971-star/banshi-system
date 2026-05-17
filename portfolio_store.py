"""
portfolio_store.py — 持倉資料持久化（GitHub API）
"""
import json
import os
import urllib.request
import urllib.error

_REPO  = os.getenv("GH_REPO", "")
_TOKEN = os.getenv("GH_PAT", "")
_PATH  = "portfolio.json"
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
            "message": "update portfolio",
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


def load_portfolio() -> list:
    content, _ = _gh_get()
    return content


def save_portfolio(positions: list) -> bool:
    _, sha = _gh_get()
    return _gh_put(positions, sha)


def add_position(stock_id: str, name: str, entry_price: float,
                 entry_date: str, shares: float, note: str = "") -> bool:
    positions = load_portfolio()
    positions = [p for p in positions if p["stock_id"] != stock_id]
    positions.append({
        "stock_id":    stock_id,
        "name":        name,
        "entry_price": float(entry_price),
        "entry_date":  entry_date,
        "shares":      float(shares),
        "note":        note,
    })
    return save_portfolio(positions)


def remove_position(stock_id: str) -> bool:
    positions = load_portfolio()
    positions = [p for p in positions if p["stock_id"] != stock_id]
    return save_portfolio(positions)

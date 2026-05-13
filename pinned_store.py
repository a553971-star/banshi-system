"""
pinned_store.py — 追蹤清單持久化（GitHub API）
取代本地 pinned.json，避免 Streamlit Cloud redeploy 後資料消失。

Streamlit secrets 需設定：
  GH_PAT  = "ghp_xxxx"              # 有 contents:write 的 PAT
  GH_REPO = "owner/banshi-system"   # 倉庫全名
"""
import base64
import json

import requests
import streamlit as st

GITHUB_API = "https://api.github.com"
FILE_PATH  = "pinned.json"


def _headers() -> dict:
    token = st.secrets.get("GH_PAT", "")
    return {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
    }


def _repo() -> str:
    return st.secrets.get("GH_REPO", "")


def load_pinned() -> set:
    """從 GitHub 讀取 pinned.json，回傳 set of stock_id strings。"""
    try:
        url  = f"{GITHUB_API}/repos/{_repo()}/contents/{FILE_PATH}"
        resp = requests.get(url, headers=_headers(), timeout=10)
        if resp.status_code == 200:
            raw  = base64.b64decode(resp.json()["content"]).decode("utf-8")
            data = json.loads(raw)
            if isinstance(data, list):
                return set(str(x) for x in data)
            if isinstance(data, dict):
                return set(str(x) for x in data.get("pinned", []))
    except Exception:
        pass
    return set()


def save_pinned(pinned: set) -> None:
    """把 pinned set 寫回 GitHub pinned.json。"""
    try:
        url  = f"{GITHUB_API}/repos/{_repo()}/contents/{FILE_PATH}"
        sha  = None
        resp = requests.get(url, headers=_headers(), timeout=10)
        if resp.status_code == 200:
            sha = resp.json().get("sha")

        content = json.dumps(sorted(pinned), ensure_ascii=False, indent=2)
        encoded = base64.b64encode(content.encode()).decode()
        payload = {"message": "update pinned.json", "content": encoded}
        if sha:
            payload["sha"] = sha

        requests.put(url, headers=_headers(), json=payload, timeout=10)
    except Exception:
        pass

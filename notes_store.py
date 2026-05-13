"""
notes_store.py — 追蹤清單備註持久化（GitHub API）
讀寫 notes.json，格式：{stock_id: note_str}

Streamlit secrets 需設定：
  GH_PAT  = "ghp_xxxx"
  GH_REPO = "owner/banshi-system"
"""
import base64
import json

import requests
import streamlit as st

GITHUB_API = "https://api.github.com"
FILE_PATH  = "notes.json"


def _headers() -> dict:
    token = st.secrets.get("GH_PAT", "")
    return {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github.v3+json",
    }


def _repo() -> str:
    return st.secrets.get("GH_REPO", "")


def load_notes() -> dict:
    """從 GitHub 讀取 notes.json，回傳 {stock_id: note_str}。"""
    try:
        url  = f"{GITHUB_API}/repos/{_repo()}/contents/{FILE_PATH}"
        resp = requests.get(url, headers=_headers(), timeout=10)
        if resp.status_code == 200:
            raw  = base64.b64decode(resp.json()["content"]).decode("utf-8")
            data = json.loads(raw)
            if isinstance(data, dict):
                return {str(k): str(v) for k, v in data.items()}
    except Exception:
        pass
    return {}


def save_notes(notes: dict) -> None:
    """把 notes dict 寫回 GitHub notes.json。"""
    try:
        url  = f"{GITHUB_API}/repos/{_repo()}/contents/{FILE_PATH}"
        sha  = None
        resp = requests.get(url, headers=_headers(), timeout=10)
        if resp.status_code == 200:
            sha = resp.json().get("sha")

        content = json.dumps(notes, ensure_ascii=False, indent=2)
        encoded = base64.b64encode(content.encode()).decode()
        payload = {"message": "update notes.json", "content": encoded}
        if sha:
            payload["sha"] = sha

        requests.put(url, headers=_headers(), json=payload, timeout=10)
    except Exception:
        pass

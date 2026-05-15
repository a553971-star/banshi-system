"""
ui/sidebar.py — 可重用的 Sidebar 即時查詢元件
用法：from ui.sidebar import render_sidebar_query
      render_sidebar_query(key_suffix="", render_result_fn=render_live_result_block)
"""
import os

import pandas as pd
import streamlit as st

from utils.config import load_params
from pinned_store import load_pinned, save_pinned

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_MAX_CACHE    = 10


def _margin_radar_tag(row, _m5d_baseline=None):
    m5d = pd.to_numeric(row.get("margin_change_5d", None), errors="coerce") if hasattr(row, "get") else None
    if m5d is None or pd.isna(m5d):
        return ""
    if m5d <= 0:
        return "💰↓ 融資退潮"
    if _m5d_baseline is None or _m5d_baseline == 0:
        return "💰 融資微升"
    if m5d >= _m5d_baseline * 2:
        return "💰💰💰 融資大火"
    elif m5d >= _m5d_baseline * 1.5:
        return "💰💰 融資升溫"
    return "💰 融資微升"


def _volume_spike_tag(row):
    if row is None:
        return ""
    try:
        vr  = float(row.get("volume_ratio") or 0)
        ret = abs(float(row.get("daily_return_pct") or 0))
        if vr >= 1.5 and ret >= 3:
            return "🔥 爆量"
    except Exception:
        pass
    return ""


def _resolve_stock_id(query: str) -> str:
    """Resolve a query (ID or name) to a stock_id. Returns query unchanged if not found."""
    sid = query.strip()

    # 1. companies.csv — by ID
    try:
        _co = pd.read_csv(os.path.join(_PROJECT_ROOT, "companies.csv"), dtype=str)
        _m  = _co[_co["stock_id"] == sid]
        if not _m.empty:
            return str(_m.iloc[0]["stock_id"])
        if "name" in _co.columns:
            _mn = _co[_co["name"].str.contains(sid, na=False)]
            if not _mn.empty:
                return str(_mn.iloc[0]["stock_id"])
    except Exception:
        pass

    # 2. stock_names.csv — by name
    try:
        _sn = pd.read_csv(os.path.join(_PROJECT_ROOT, "stock_names.csv"), dtype=str)
        _sm = _sn[_sn["name"].str.contains(sid, na=False)]
        if not _sm.empty:
            return str(_sm.iloc[0]["stock_id"])
    except Exception:
        pass

    # 3. latest_decisions_universe.csv — by name
    try:
        _udf = pd.read_csv(os.path.join(_PROJECT_ROOT, "latest_decisions_universe.csv"), dtype=str)
        _um  = _udf[_udf["name"].str.contains(sid, na=False)]
        if not _um.empty:
            return str(_um.iloc[0]["stock_id"])
    except Exception:
        pass

    return sid


def render_sidebar_query(key_suffix: str = "", render_result_fn=None) -> None:
    """Render sidebar live query widget + cached results section.

    Parameters
    ----------
    key_suffix:       String appended to all Streamlit widget keys to avoid
                      collisions when used on multiple pages.
    render_result_fn: Callable(stock_id, result_dict) to render a result card.
                      Falls back to st.json if None.
    """
    cache_key    = f"live_cache{key_suffix}"
    trigger_key  = f"sidebar_trigger{key_suffix}"
    input_key    = f"sidebar_live_query{key_suffix}"
    btn_key      = f"sidebar_live_btn{key_suffix}"
    clear_key    = f"sidebar_clear{key_suffix}"

    if cache_key not in st.session_state:
        st.session_state[cache_key] = {}

    # ── Sidebar widgets ───────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("### 🔬 即時個股查詢")
        _sb_input = st.text_input(
            "股票代號或名稱",
            placeholder="例：2330 或 台積電",
            key=input_key,
        )
        if st.button("查詢", key=btn_key, type="primary", use_container_width=True):
            if _sb_input.strip():
                st.session_state[trigger_key] = _sb_input.strip()
        if st.button("🗑️ 清除所有查詢", key=clear_key, use_container_width=True):
            st.session_state[cache_key] = {}
            st.rerun()

    # ── Trigger query ─────────────────────────────────────────────────────────
    _trigger = st.session_state.pop(trigger_key, None)
    if _trigger:
        from live_analyzer import process_stock_live
        _live_id = _resolve_stock_id(_trigger)

        with st.spinner(f"正在分析 {_live_id}..."):
            try:
                _params = load_params()
                _result = process_stock_live(_live_id, _params, print_snapshot=False)
            except Exception as _e:
                st.sidebar.error(f"分析例外：{_e}")
                _result = None

        if _result is not None:
            _cache = st.session_state[cache_key]
            _cache.pop(_live_id, None)
            _cache[_live_id] = {
                "result": _result,
                "name":   _result.get("name", _live_id),
                "ts":     pd.Timestamp.now().strftime("%H:%M:%S"),
            }
            while len(_cache) > _MAX_CACHE:
                _cache.pop(next(iter(_cache)))
            st.rerun()
        else:
            st.sidebar.error(f"查無資料：{_live_id}，請確認代號是否正確")

    # ── Cache display ─────────────────────────────────────────────────────────
    st.subheader("🔬 全市場即時個股分析")
    if not st.session_state.get(cache_key):
        st.caption("在左側 Sidebar 輸入股票代號或名稱查詢")
        return

    for _sid, _cdata in reversed(list(st.session_state[cache_key].items())):
        _cresult = _cdata.get("result") if isinstance(_cdata, dict) else _cdata
        _cname   = _cdata.get("name", _sid) if isinstance(_cdata, dict) else (
            _cresult.get("name", _sid) if _cresult else _sid
        )
        _cts     = _cdata.get("ts", "") if isinstance(_cdata, dict) else ""
        _dec     = _cresult.get("decision", "N/A") if _cresult else "N/A"
        _dec_icon = {"BUY": "🟢", "WAIT": "🟡", "IGNORE": "⚪", "SELL": "🔴"}.get(_dec, "⚪")
        _cmtag   = _margin_radar_tag(_cresult) if _cresult else ""
        _cvtag   = _volume_spike_tag(_cresult) if _cresult else ""

        _col1, _col2, _col3 = st.columns([8, 1, 1])
        with _col1:
            with st.expander(
                f"{_dec_icon} {_sid} {_cname} — {_dec}　{_cmtag}　{_cvtag}　🕐 {_cts}",
                expanded=False,
            ):
                if _cresult:
                    _m5d  = float(_cresult.get("margin_change_5d") or 0)
                    _mchg = float(_cresult.get("margin_change_pct") or 0)
                    _mcon = int(_cresult.get("margin_consecutive_increase") or 0)
                    _mc1, _mc2, _mc3 = st.columns(3)
                    with _mc1:
                        st.metric("融資5日增減", f"{_m5d:+,.0f} 張")
                    with _mc2:
                        st.metric("融資增幅", f"{_mchg:+.1f}%")
                    with _mc3:
                        st.metric("融資連增天數", f"{_mcon} 天")
                    if render_result_fn is not None:
                        render_result_fn(_sid, _cresult)
                    else:
                        st.json(_cresult)
                else:
                    st.warning("無法取得資料")

        with _col2:
            st.markdown("<div style='margin-top:8px'></div>", unsafe_allow_html=True)
            if st.button("✕", key=f"cache_rm_{key_suffix}_{_sid}"):
                del st.session_state[cache_key][_sid]
                st.rerun()

        with _col3:
            st.markdown("<div style='margin-top:8px'></div>", unsafe_allow_html=True)
            _is_pinned  = _sid in st.session_state.get("pinned", {})
            _pin_label  = "📌" if _is_pinned else "➕"
            if st.button(_pin_label, key=f"cache_pin_{key_suffix}_{_sid}", help="加入追蹤清單"):
                _pinned = load_pinned()
                if _sid in _pinned:
                    _pinned.discard(_sid)
                else:
                    _pinned.add(_sid)
                save_pinned(_pinned)
                st.session_state["pinned"] = _pinned
                st.rerun()

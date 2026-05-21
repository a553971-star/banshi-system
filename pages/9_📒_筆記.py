"""
pages/9_📒_筆記.py
研究筆記本 — 分主題管理股票觀察筆記。
"""
import os
import sys

import streamlit as st

BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_PATH)

from notes_book_store import (
    load_notes_book,
    save_notes_book,
    add_section,
    rename_section,
    delete_section,
    add_stock,
    update_note,
    remove_stock,
)

st.set_page_config(page_title="研究筆記本", layout="wide")
st.title("📒 研究筆記本")
st.caption("分主題管理你的股票觀察筆記")

# ── 載入資料 ──────────────────────────────────────────────────────────────────
sections = load_notes_book()

# ── 新增主題 ──────────────────────────────────────────────────────────────────
with st.expander("➕ 新增主題", expanded=not sections):
    with st.form("new_section_form", clear_on_submit=True):
        new_title = st.text_input("主題名稱", placeholder="例：等回檔、觀察中、AI 概念")
        submitted = st.form_submit_button("✅ 新增主題", type="primary")
        if submitted:
            if new_title.strip():
                ok = add_section(new_title.strip())
                if ok:
                    st.success(f"已新增「{new_title.strip()}」")
                    st.rerun()
                else:
                    st.error("儲存失敗，請確認 GH_PAT 設定")
            else:
                st.warning("請輸入主題名稱")

if not sections:
    st.info("尚無主題，請先新增")
    st.stop()

# ── 各主題區塊 ────────────────────────────────────────────────────────────────
for sec in sections:
    sec_id    = sec["id"]
    sec_title = sec["title"]
    stocks    = sec.get("stocks", [])

    with st.expander(f"📂 {sec_title}　（{len(stocks)} 支）", expanded=True):

        # ── 主題操作列 ────────────────────────────────────────────────────────
        col_rename, col_del = st.columns([3, 1])
        with col_rename:
            with st.form(f"rename_{sec_id}", clear_on_submit=True):
                new_name = st.text_input(
                    "重新命名主題", value=sec_title,
                    label_visibility="collapsed",
                    placeholder="輸入新名稱後按 Enter"
                )
                if st.form_submit_button("✏️ 重新命名"):
                    if new_name.strip() and new_name.strip() != sec_title:
                        ok = rename_section(sec_id, new_name.strip())
                        if ok:
                            st.rerun()
        with col_del:
            if st.button(
                "🗑️ 刪除主題", key=f"del_sec_{sec_id}",
                help="刪除整個主題（含所有股票）",
                use_container_width=True,
            ):
                st.session_state[f"confirm_del_{sec_id}"] = True

        if st.session_state.get(f"confirm_del_{sec_id}"):
            st.warning(f"確定要刪除「{sec_title}」及其所有筆記嗎？")
            c1, c2 = st.columns(2)
            if c1.button("✅ 確定刪除", key=f"yes_del_{sec_id}"):
                delete_section(sec_id)
                st.session_state.pop(f"confirm_del_{sec_id}", None)
                st.rerun()
            if c2.button("❌ 取消", key=f"no_del_{sec_id}"):
                st.session_state.pop(f"confirm_del_{sec_id}", None)
                st.rerun()

        st.divider()

        # ── 新增股票到此主題 ──────────────────────────────────────────────────
        with st.form(f"add_stock_{sec_id}", clear_on_submit=True):
            c1, c2, c3 = st.columns([1, 2, 1])
            with c1:
                inp_id = st.text_input("股票代號", placeholder="2330")
            with c2:
                inp_note = st.text_input("筆記", placeholder="等跌到 900 再進")
            with c3:
                st.write("")  # spacing
                add_btn = st.form_submit_button("➕ 加入", use_container_width=True)

            if add_btn:
                if inp_id.strip():
                    # 嘗試抓即時名稱與價格
                    _name  = inp_id.strip()
                    _price = None
                    try:
                        from utils.config import load_params
                        from main import process_stock_live
                        _r = process_stock_live(inp_id.strip(), load_params())
                        if _r:
                            _name  = _r.get("name") or _name
                            _price = _r.get("close") or _r.get("current_price")
                    except Exception:
                        pass

                    ok = add_stock(
                        section_id=sec_id,
                        stock_id=inp_id.strip(),
                        name=_name,
                        note=inp_note,
                        created_price=_price,
                    )
                    if ok:
                        st.success(f"已加入 {_name}（{inp_id.strip()}）")
                        st.rerun()
                    else:
                        st.error("儲存失敗，請確認 GH_PAT 設定")
                else:
                    st.warning("請輸入股票代號")

        # ── 股票列表 ──────────────────────────────────────────────────────────
        if not stocks:
            st.caption("此主題尚無股票")
        else:
            for st_item in stocks:
                sid       = st_item["stock_id"]
                sname     = st_item.get("name", sid)
                snote     = st_item.get("note", "")
                sprice    = st_item.get("created_price")
                sadded_at = st_item.get("added_at", "")

                with st.container(border=True):
                    row1, row_del = st.columns([8, 1])
                    with row1:
                        price_str = f"　加入時 ${sprice:,.1f}" if sprice else ""
                        st.markdown(
                            f"**{sid}** {sname}　"
                            f"<span style='color:gray;font-size:0.85em'>{sadded_at}{price_str}</span>",
                            unsafe_allow_html=True,
                        )
                    with row_del:
                        if st.button("✕", key=f"rm_{sec_id}_{sid}", help=f"移除 {sid}"):
                            remove_stock(sec_id, sid)
                            st.rerun()

                    # 筆記編輯
                    with st.form(f"note_{sec_id}_{sid}", clear_on_submit=False):
                        new_note = st.text_input(
                            "筆記", value=snote,
                            label_visibility="collapsed",
                            placeholder="輸入筆記後按 Enter 儲存",
                        )
                        if st.form_submit_button("💾 儲存筆記"):
                            if new_note != snote:
                                ok = update_note(sec_id, sid, new_note)
                                if ok:
                                    st.rerun()
                                else:
                                    st.error("儲存失敗")

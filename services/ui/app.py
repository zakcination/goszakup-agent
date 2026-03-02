"""
services/ui/app.py
------------------
Submission-grade Streamlit UI for goszakup-agent demo.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any

import httpx
import pandas as pd
import streamlit as st


AGENT_API_URL = os.environ.get("AGENT_API_URL", "http://localhost:8002")
ANALYTICS_API_URL = os.environ.get("ANALYTICS_API_URL", "http://localhost:8001")

st.set_page_config(
    page_title="Goszakup AI Submission Demo",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="expanded",
)

THEME_DARK = {
    "bg": "#0a1018",
    "panel": "#111a25",
    "panel_alt": "#0d151e",
    "text": "#e6edf7",
    "muted": "#92a2b6",
    "accent": "#24a1ff",
    "accent_2": "#ffc857",
    "ok": "#20c997",
    "warn": "#ffb703",
    "danger": "#ff5a5f",
    "border": "rgba(146, 162, 182, 0.26)",
}

THEME_LIGHT = {
    "bg": "#f2f6fb",
    "panel": "#ffffff",
    "panel_alt": "#edf2f9",
    "text": "#0a1220",
    "muted": "#5f7289",
    "accent": "#0068c9",
    "accent_2": "#c28500",
    "ok": "#0f9d58",
    "warn": "#d97706",
    "danger": "#d11a2a",
    "border": "rgba(95, 114, 137, 0.28)",
}

if "theme" not in st.session_state:
    st.session_state.theme = "Dark"
if "question" not in st.session_state:
    st.session_state.question = ""
if "last_response" not in st.session_state:
    st.session_state.last_response = None
if "history" not in st.session_state:
    st.session_state.history = []


palette = THEME_DARK if st.session_state.theme == "Dark" else THEME_LIGHT


st.markdown(
    f"""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@300;400;500;600;700&family=IBM+Plex+Mono:wght@400;600&display=swap');

    :root {{
      --bg: {palette['bg']};
      --panel: {palette['panel']};
      --panel-alt: {palette['panel_alt']};
      --text: {palette['text']};
      --muted: {palette['muted']};
      --accent: {palette['accent']};
      --accent-2: {palette['accent_2']};
      --ok: {palette['ok']};
      --warn: {palette['warn']};
      --danger: {palette['danger']};
      --border: {palette['border']};
    }}

    html, body, [class*="css"] {{
      font-family: "IBM Plex Sans", system-ui, sans-serif;
      color: var(--text);
    }}

    [data-testid="stAppViewContainer"] {{
      background:
        radial-gradient(900px 400px at 0% 0%, rgba(36,161,255,0.10), transparent 60%),
        radial-gradient(900px 400px at 100% 100%, rgba(255,200,87,0.08), transparent 60%),
        var(--bg);
    }}

    [data-testid="stSidebar"] {{
      background: var(--panel-alt);
      border-right: 1px solid var(--border);
    }}

    .hero {{
      background: linear-gradient(135deg, rgba(8,18,32,0.95) 0%, rgba(14,24,38,0.92) 100%);
      border: 1px solid var(--border);
      border-radius: 18px;
      padding: 18px 22px;
      box-shadow: 0 20px 48px rgba(3,10,22,0.35);
      margin-bottom: 14px;
    }}

    .hero h1 {{
      margin: 0;
      font-size: 26px;
      font-weight: 700;
      letter-spacing: 0.2px;
      color: #eaf2ff;
    }}

    .hero p {{
      margin-top: 6px;
      margin-bottom: 0;
      color: #b2c0d2;
      font-size: 13px;
    }}

    .chip {{
      display: inline-flex;
      align-items: center;
      padding: 3px 10px;
      border: 1px solid var(--border);
      border-radius: 999px;
      background: rgba(146,162,182,0.14);
      color: var(--muted);
      font-size: 11px;
      margin-right: 6px;
    }}

    .panel {{
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 14px 16px;
      margin-bottom: 12px;
      box-shadow: 0 10px 24px rgba(4,13,24,0.14);
    }}

    .panel-title {{
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--muted);
      margin-bottom: 8px;
      font-weight: 600;
    }}

    .metric-value {{
      font-family: "IBM Plex Mono", ui-monospace, Menlo, Consolas, monospace;
      font-size: 14px;
    }}

    .verdict {{
      display: inline-flex;
      align-items: center;
      border-radius: 999px;
      padding: 7px 12px;
      font-weight: 600;
      font-size: 12px;
      border: 1px solid transparent;
    }}

    .v-ok {{ background: rgba(32,201,151,0.15); color: var(--ok); border-color: rgba(32,201,151,0.35); }}
    .v-anomaly {{ background: rgba(255,90,95,0.15); color: var(--danger); border-color: rgba(255,90,95,0.35); }}
    .v-insufficient {{ background: rgba(146,162,182,0.15); color: var(--text); border-color: var(--border); }}

    .kpi-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
      gap: 10px;
    }}

    .kpi {{
      background: var(--panel-alt);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 10px 12px;
    }}

    .kpi-label {{
      font-size: 10px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: var(--muted);
    }}

    .kpi-num {{
      font-family: "IBM Plex Mono", ui-monospace, Menlo, Consolas, monospace;
      font-size: 14px;
      margin-top: 5px;
    }}

    .topk-card {{
      border: 1px solid var(--border);
      border-radius: 12px;
      background: var(--panel-alt);
      padding: 12px;
      margin-bottom: 10px;
    }}

    .topk-title {{
      font-weight: 600;
      margin-bottom: 6px;
      font-size: 13px;
    }}

    .small {{
      color: var(--muted);
      font-size: 12px;
    }}
    </style>
    """,
    unsafe_allow_html=True,
)


def _set_prompt(text: str) -> None:
    st.session_state.question = text


def _fmt(v: Any) -> str:
    if v is None:
        return "—"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return f"{v:,.2f}".rstrip("0").rstrip(".")
    if isinstance(v, str):
        return v
    return str(v)


def _compact(d: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in d.items() if v not in (None, "", [], {}, "—")}


def _verdict_class(verdict: str) -> str:
    if verdict in {"ok", "found", "found_0"}:
        return "v-ok"
    if verdict == "anomaly" or str(verdict).startswith("found_"):
        return "v-anomaly"
    return "v-insufficient"


def _summary_for_submission(resp: dict[str, Any]) -> str:
    verdict = resp.get("verdict", "insufficient_data")
    analytics = resp.get("analytics", {}) or {}
    n = analytics.get("sample_n") or analytics.get("n")
    method = analytics.get("method") or "—"
    if verdict == "insufficient_data":
        reason = analytics.get("reason") or "Выборка недостаточна"
        return f"Вердикт: INSUFFICIENT_DATA. Причина: {reason}."
    fp = analytics.get("fair_price")
    dev = analytics.get("deviation_pct")
    if fp is not None and dev is not None:
        return f"Вердикт: {verdict}. Оценка справедливой цены={_fmt(fp)}; отклонение={_fmt(dev)}%."
    return f"Вердикт: {verdict}. Метод: {method}. Объем выборки N={_fmt(n)}."


def _render_kv(title: str, d: dict[str, Any], cols: int = 3) -> None:
    st.markdown(f"<div class='panel-title'>{title}</div>", unsafe_allow_html=True)
    d = _compact(d)
    if not d:
        st.caption("Нет данных.")
        return
    items = list(d.items())
    c = st.columns(cols)
    for i, (k, v) in enumerate(items):
        with c[i % cols]:
            st.caption(k)
            if isinstance(v, (dict, list)):
                st.code(json.dumps(v, ensure_ascii=False, indent=2), language="json")
            else:
                st.markdown(f"<span class='metric-value'>{_fmt(v)}</span>", unsafe_allow_html=True)


def _render_topk_cards(items: list[dict[str, Any]], limit: int) -> None:
    if not items:
        st.caption("Нет Top‑K примеров.")
        return

    for idx, item in enumerate(items[:limit], start=1):
        contract_id = item.get("contract_id")
        lot_id = item.get("lot_id")
        enstru = item.get("enstru_code")
        year = item.get("year") or item.get("item_year")

        if contract_id:
            title = f"#{idx} · Contract {contract_id}"
        elif lot_id:
            title = f"#{idx} · Lot {lot_id}"
        elif enstru:
            title = f"#{idx} · ENSTRU {enstru}"
        else:
            title = f"#{idx} · Evidence"

        st.markdown(f"<div class='topk-card'><div class='topk-title'>{title}</div>", unsafe_allow_html=True)

        c1, c2, c3 = st.columns(3)
        with c1:
            if enstru:
                st.caption("ENSTRU")
                st.write(enstru)
            if item.get("customer_bin"):
                st.caption("Customer BIN")
                st.write(item.get("customer_bin"))
        with c2:
            if item.get("unit_price") is not None:
                st.caption("Unit Price")
                st.write(_fmt(item.get("unit_price")))
            if item.get("total_price") is not None:
                st.caption("Total Price")
                st.write(_fmt(item.get("total_price")))
            if item.get("deviation_pct") is not None:
                st.caption("Deviation %")
                st.write(_fmt(item.get("deviation_pct")))
        with c3:
            if year is not None:
                st.caption("Year")
                st.write(_fmt(year))
            if item.get("n") is not None:
                st.caption("Sample N")
                st.write(_fmt(item.get("n")))
            if item.get("ratio") is not None:
                st.caption("YoY Ratio")
                st.write(f"{_fmt(item.get('ratio'))}x")

        links = []
        for lk in ("contract_api_url", "trd_buy_api_url", "portal_announce_url", "plan_point_api_url"):
            if item.get(lk):
                links.append((lk, item.get(lk)))

        if links:
            st.caption("Links")
            for lk, url in links[:4]:
                st.markdown(f"- [{lk}]({url})")

        st.markdown("</div>", unsafe_allow_html=True)


def _render_anomaly_list(items: list[dict[str, Any]]) -> None:
    if not items:
        st.caption("Нет аномалий для отображения.")
        return

    df = pd.DataFrame(items)
    if df.empty:
        st.caption("Нет аномалий для отображения.")
        return

    if "deviation_pct" in df.columns:
        df = df.sort_values(by="deviation_pct", ascending=False, na_position="last")

    show_cols = [
        "contract_id",
        "contract_item_id",
        "customer_bin",
        "customer_name_ru",
        "supplier_bin",
        "supplier_name_ru",
        "supplier_is_rnu",
        "enstru_code",
        "enstru_name_ru",
        "item_year",
        "deviation_pct",
        "actual_unit_price",
        "expected_unit_price",
        "ref_n",
        "confidence",
        "fallback_level",
        "kato_delivery",
        "delivery_place_name",
        "contract_number",
        "number_anno",
        "sign_date",
    ]
    cols = [c for c in show_cols if c in df.columns]
    v = df[cols].copy()
    if "deviation_pct" in v.columns:
        v["deviation_pct"] = v["deviation_pct"].round(2)

    col_map = {
        "contract_id": "Contract ID",
        "contract_item_id": "Item ID",
        "customer_bin": "Customer BIN",
        "customer_name_ru": "Customer Name",
        "supplier_bin": "Supplier BIN",
        "supplier_name_ru": "Supplier Name",
        "supplier_is_rnu": "Supplier RNU",
        "enstru_code": "ENSTRU",
        "enstru_name_ru": "ENSTRU Name",
        "item_year": "Year",
        "deviation_pct": "Deviation %",
        "actual_unit_price": "Actual Price",
        "expected_unit_price": "Expected Price",
        "ref_n": "Ref N",
        "confidence": "Confidence",
        "fallback_level": "Fallback",
        "kato_delivery": "KATO",
        "delivery_place_name": "Delivery Place",
        "contract_number": "Contract Number",
        "number_anno": "Announcement No",
        "sign_date": "Sign Date",
    }
    v = v.rename(columns=col_map)
    st.dataframe(v, use_container_width=True, height=460)

    metrics_cols = st.columns(4)
    with metrics_cols[0]:
        st.metric("Rows", len(df))
    with metrics_cols[1]:
        st.metric("Unique customers", int(df["customer_bin"].nunique()) if "customer_bin" in df.columns else 0)
    with metrics_cols[2]:
        st.metric("Unique suppliers", int(df["supplier_bin"].nunique()) if "supplier_bin" in df.columns else 0)
    with metrics_cols[3]:
        if "deviation_pct" in df.columns and len(df):
            st.metric("Max deviation %", _fmt(float(df["deviation_pct"].max())))
        else:
            st.metric("Max deviation %", "—")


def _append_history(question: str, response: dict[str, Any]) -> None:
    st.session_state.history.insert(
        0,
        {
            "ts": datetime.now().strftime("%H:%M:%S"),
            "q": question,
            "intent": response.get("intent"),
            "verdict": response.get("verdict"),
        },
    )
    st.session_state.history = st.session_state.history[:15]


@st.cache_data(ttl=300)
def _fetch_suggestions() -> list[str]:
    prompts: list[str] = []

    # 1) anomalies from analytics
    try:
        r = httpx.post(
            f"{ANALYTICS_API_URL}/anomaly",
            json={"min_deviation": 30.0, "limit": 3},
            timeout=10,
        )
        if r.status_code == 200:
            for it in r.json().get("items", []):
                enstru = it.get("enstru_code")
                year = it.get("item_year") or it.get("year")
                if enstru and year:
                    prompts.append(f"Найди закупки с отклонением цены >30% по ЕНСТРУ {enstru} за {year} год")
    except Exception:
        pass

    # 2) market coverage with enough N
    try:
        r = httpx.post(
            f"{ANALYTICS_API_URL}/search",
            json={"mode": "market", "min_n": 20, "sort_by": "n", "limit": 3},
            timeout=10,
        )
        if r.status_code == 200:
            for it in r.json().get("items", []):
                enstru = it.get("enstru_code")
                year = it.get("year")
                if enstru and year:
                    prompts.append(f"Оцени справедливость цены по ЕНСТРУ {enstru} за {year} в идентичном городе поставки")
    except Exception:
        pass

    return list(dict.fromkeys(prompts))


# Sidebar controls
with st.sidebar:
    st.header("Display")
    st.session_state.theme = st.radio("Theme", ["Dark", "Light"], index=0 if st.session_state.theme == "Dark" else 1, horizontal=True)
    st.divider()

    st.header("Submission Presets")
    st.caption("Обязательные классы запросов из ТЗ + реальные кейсы из ваших артефактов")

    preset_groups = {
        "Case 1 · Fairness": [
            "Оцени адекватность цены лота № 32899983 относительно аналогичных контрактов других ведомств в идентичном городе поставки",
            "Оцени адекватность цены лота № 16446063 относительно аналогичных контрактов в идентичном городе поставки",
        ],
        "Case 2 · Anomalies": [
            "Найди закупки с отклонением цены >30% по ЕНСТРУ 331312.100.000000 за 2024 год",
            "Подозрительные закупки за 2025 по 100140011059",
        ],
        "Case 3 · Volume": [
            "Выяви нетипичное завышение количества ТРУ по сравнению с предыдущими годами по ЕНСТРУ 172213.000.000002 за 2025 год",
            "Что в плане, но нет договора по 100140011059 за 2026",
        ],
        "Audit & Control": [
            "История договора №24764966",
            "Сводка закупок по 100140011059 за 2025",
            "Топ-10 дорогих лотов в 2025",
        ],
    }

    for grp, prompts in preset_groups.items():
        with st.expander(grp, expanded=(grp == "Case 1 · Fairness")):
            for i, p in enumerate(prompts):
                st.button(p, key=f"preset_{grp}_{i}", on_click=_set_prompt, args=(p,))

    st.divider()
    st.header("Dynamic Ideas")
    suggestions = _fetch_suggestions()
    if suggestions:
        for i, s in enumerate(suggestions):
            st.button(s, key=f"dyn_{i}", on_click=_set_prompt, args=(s,))
    else:
        st.caption("Нет динамических подсказок из analytics API.")

    st.divider()
    st.header("Prompt Tips")
    st.markdown(
        """
- Указывайте `ЕНСТРУ`, `год`, при необходимости `БИН`.
- Для лота используйте формат: `лот № 32899983`.
- Для аномалий добавляйте порог: `>30%`.
- Для устойчивости выборки требуйте `N>=20`.
"""
    )


# Header
st.markdown(
    """
    <div class="hero">
      <h1>Goszakup AI · Submission Console</h1>
      <p>Intent → Whitelisted Tool → Verified Analytics → Explainable Verdict (L0→L4)</p>
    </div>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <span class="chip">8 intents</span>
    <span class="chip">No free SQL</span>
    <span class="chip">Evidence-first output</span>
    <span class="chip">Top-K with IDs/links</span>
    """,
    unsafe_allow_html=True,
)


left, right = st.columns([2.2, 1.2])
with left:
    with st.form("ask_form", clear_on_submit=False):
        st.text_area("Введите вопрос (RU/KZ)", height=130, key="question")
        run = st.form_submit_button("Run Analysis")

with right:
    st.markdown("<div class='panel'><div class='panel-title'>Recent Queries</div>", unsafe_allow_html=True)
    if st.session_state.history:
        for row in st.session_state.history[:8]:
            st.caption(f"[{row['ts']}] {row['intent']} · {row['verdict']}")
            st.write(row["q"])
    else:
        st.caption("История пустая.")
    st.markdown("</div>", unsafe_allow_html=True)


if run:
    q = st.session_state.question.strip()
    if not q:
        st.warning("Введите вопрос.")
    else:
        try:
            with st.spinner("Выполняется tool-driven анализ..."):
                resp = httpx.post(f"{AGENT_API_URL}/ask", json={"question": q}, timeout=90)
                resp.raise_for_status()
                data = resp.json()
                st.session_state.last_response = data
                _append_history(q, data)
        except Exception as e:
            st.error(f"API error: {e}")


data = st.session_state.last_response
if data:
    verdict = data.get("verdict", "insufficient_data")
    confidence = data.get("confidence", "—")
    intent = data.get("intent", "—")
    v_cls = _verdict_class(verdict)

    st.markdown("<div class='panel'>", unsafe_allow_html=True)
    st.markdown(
        f"<span class='verdict {v_cls}'>{verdict}</span> <span class='small' style='margin-left:10px;'>Intent: {intent} · Confidence: {confidence}</span>",
        unsafe_allow_html=True,
    )
    st.markdown(f"**L0 / Verdict:** {_summary_for_submission(data)}")
    st.markdown("</div>", unsafe_allow_html=True)

    analytics = data.get("analytics", {}) or {}
    parameters = data.get("parameters", {}) or {}
    meta = data.get("meta", {}) or {}

    kpis = [
        ("Sample N", analytics.get("sample_n")),
        ("Method", analytics.get("method")),
        ("Scope", analytics.get("scope_level")),
        ("Fair Price", analytics.get("fair_price")),
        ("Deviation %", analytics.get("deviation_pct")),
        ("Cache", meta.get("cache")),
        ("Tool", meta.get("tool_name")),
        ("Freshness", meta.get("freshness_ts")),
    ]
    kpis = [(k, v) for k, v in kpis if v not in (None, "", [], {}, "—")]

    if kpis:
        st.markdown("<div class='panel'><div class='panel-title'>Core Metrics</div><div class='kpi-grid'>", unsafe_allow_html=True)
        for label, val in kpis:
            st.markdown(
                f"<div class='kpi'><div class='kpi-label'>{label}</div><div class='kpi-num'>{_fmt(val)}</div></div>",
                unsafe_allow_html=True,
            )
        st.markdown("</div></div>", unsafe_allow_html=True)

    tabs = st.tabs([
        "L1·Parameters",
        "L2·Analytics",
        "L3·Top-K",
        "L4·Evidence & Limits",
        "Raw JSON",
    ])

    with tabs[0]:
        st.markdown("<div class='panel'>", unsafe_allow_html=True)
        _render_kv("Параметры поиска", parameters, cols=3)
        st.markdown("</div>", unsafe_allow_html=True)

    with tabs[1]:
        st.markdown("<div class='panel'>", unsafe_allow_html=True)
        _render_kv("Аналитика", analytics, cols=3)
        st.markdown("</div>", unsafe_allow_html=True)

        l2 = data.get("l2") or {}
        if l2:
            st.markdown("<div class='panel'>", unsafe_allow_html=True)
            _render_kv("L2 Breakdown", l2, cols=2)
            st.markdown("</div>", unsafe_allow_html=True)

    with tabs[2]:
        top_k = data.get("top_k", []) or []
        if top_k:
            intent_name = str(data.get("intent") or "")
            is_anomaly = intent_name == "ANOMALY_SCAN" or (
                data.get("analytics", {}).get("dimension") == "anomalies"
            )
            if is_anomaly and any("deviation_pct" in row for row in top_k if isinstance(row, dict)):
                _render_anomaly_list(top_k)
            else:
                max_items = st.slider("Top-K cards", 3, min(30, len(top_k)), min(10, len(top_k)))
                _render_topk_cards(top_k, max_items)
        else:
            st.caption("Top‑K пустой для текущего запроса.")

    with tabs[3]:
        l4 = data.get("l4") or {}
        follow_ups = data.get("follow_ups") or []
        st.markdown("<div class='panel'>", unsafe_allow_html=True)
        _render_kv("Ограничения", {"limitations": meta.get("limitations", []), "confidence": confidence})
        st.markdown("</div>", unsafe_allow_html=True)

        st.markdown("<div class='panel'>", unsafe_allow_html=True)
        _render_kv("Evidence", {"evidence": meta.get("evidence", {}), "l4": l4}, cols=2)
        st.markdown("</div>", unsafe_allow_html=True)

        if follow_ups:
            st.markdown("<div class='panel'><div class='panel-title'>Follow-up Actions</div>", unsafe_allow_html=True)
            for i, f in enumerate(follow_ups):
                p = f.get("prompt")
                if p:
                    st.button(p, key=f"followup_{i}", on_click=_set_prompt, args=(p,))
            st.markdown("</div>", unsafe_allow_html=True)

    with tabs[4]:
        st.json(data)

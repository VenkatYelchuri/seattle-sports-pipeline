from __future__ import annotations

from datetime import datetime
from html import escape

import time
import pandas as pd
import requests
import streamlit as st

def render_skeleton():
    st.markdown("### Loading programs...")

    cols = st.columns(4)

    for i in range(8):  # number of skeleton cards
        col = cols[i % 4]
        with col:
            st.markdown(
                """
                <div style="
                    height:180px;
                    border-radius:10px;
                    background: linear-gradient(
                        90deg,
                        #1e293b 25%,
                        #334155 37%,
                        #1e293b 63%
                    );
                    background-size: 400% 100%;
                    animation: shimmer 1.4s ease infinite;
                    margin-bottom:10px;
                "></div>

                <style>
                @keyframes shimmer {
                    0% { background-position: -400px 0; }
                    100% { background-position: 400px 0; }
                }
                </style>
                """,
                unsafe_allow_html=True
            )

API_BASE = "http://127.0.0.1:8000"
DAY_ORDER = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
DAY_TO_NUM = {day: index for index, day in enumerate(DAY_ORDER)}
DEDUP_COLUMNS = ["program", "center", "day_of_week", "start_time", "end_time", "age_min", "age_max"]
SPORT_BADGES = {
    "badminton": "BD", "basketball": "BB", "volleyball": "VB", "soccer": "SC",
    "tennis": "TN", "pickleball": "PK", "futsal": "FS", "dodgeball": "DG",
    "handball": "HB", "yoga": "YG", "fitness": "FT", "dance": "DN",
    "martial": "MA", "juggling": "JG", "default": "SP",
}
FILTER_DEFAULTS = {
    "search": "",
    "program": "All Programs",
    "center": "All Centers",
    "day": "All Days",
    "sort": "Day then time",
}

st.set_page_config(page_title="Seattle Community Sports", page_icon="SP", layout="wide")


@st.cache_data(ttl=60)
def fetch_programs(params=None):
    try:
        res = requests.get(f"{API_BASE}/programs", params=params, timeout=8)
        res.raise_for_status()
        return normalize_programs(pd.DataFrame(res.json()))
    except Exception as e:
        st.error(f"API error: {e}")
        return pd.DataFrame()

def normalize_centers(df):
    df = df.copy()

    df["center"] = df["Center Name"].str.strip().str.lower()
    df["lat"] = df["Latitude"]
    df["lon"] = df["Longitude"]

    return df[["center", "lat", "lon"]]

def normalize_programs(df: pd.DataFrame) -> pd.DataFrame:
    for col in DEDUP_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df.copy()
    df["program"] = df["program"].fillna("Unknown Program").astype(str).str.strip()
    df["center"] = df["center"].fillna("Unknown Center").astype(str).str.strip()
    df["day_of_week"] = df["day_of_week"].fillna("Unscheduled").astype(str).str.strip()
    df["start_time"] = df["start_time"].fillna("").astype(str).str.slice(0, 5)
    df["end_time"] = df["end_time"].fillna("").astype(str).str.slice(0, 5)
    df = df.drop_duplicates(subset=[c for c in DEDUP_COLUMNS if c in df.columns], keep="first")
    df["_day_order"] = df["day_of_week"].map(DAY_TO_NUM).fillna(99).astype(int)
    df["_time_order"] = df["start_time"].apply(time_to_minutes)
    df["_search_blob"] = (df["program"] + " " + df["center"] + " " + df["day_of_week"]).str.lower()
    return df


def time_to_minutes(value) -> int:
    try:
        hour, minute = str(value).split(":")[:2]
        return int(hour) * 60 + int(minute)
    except Exception:
        return 9999


def fmt_time(value) -> str:
    value = str(value or "").strip()
    if not value:
        return "TBD"
    try:
        return datetime.strptime(value[:5], "%H:%M").strftime("%I:%M %p").lstrip("0")
    except ValueError:
        return value

def load_centers():
    try:
        df = pd.read_csv("data/processed/centers_with_websites.csv")
        return df
    except Exception as e:
        st.warning(f"Could not load centers dataset: {e}")
        return pd.DataFrame()

def age_label(value) -> str:
    if pd.isna(value) or value in (None, ""):
        return "All ages"
    try:
        return f"Ages {int(float(value))}+"
    except Exception:
        return str(value)


def sport_badge(program: str) -> str:
    lower = str(program).lower()
    for key, badge in SPORT_BADGES.items():
        if key in lower:
            return badge
    return SPORT_BADGES["default"]


def theme_palette(theme: str) -> dict[str, str]:
    if theme == "Light":
        return {
            "bg": "#edf6f8", "panel": "#ffffff", "line": "#bfd4dd",
            "text": "#10212b", "muted": "#536b78", "cyan": "#087f9b",
            "green": "#1f8f55", "input": "#ffffff",
            "hero": "linear-gradient(135deg,rgba(9,89,110,.94),rgba(37,126,88,.88))",
            "card": "linear-gradient(180deg,rgba(255,255,255,.98),rgba(239,248,250,.98))",
        }
    return {
        "bg": "#060b12", "panel": "#0d1624", "line": "#243247",
        "text": "#edf6ff", "muted": "#93a7bc", "cyan": "#25d0ff",
        "green": "#33f078", "input": "#101c2c",
        "hero": "linear-gradient(135deg,rgba(17,29,47,.96),rgba(8,78,91,.8))",
        "card": "linear-gradient(180deg,rgba(17,29,47,.96),rgba(13,22,36,.96))",
    }


def inject_css(theme: str) -> None:
    p = theme_palette(theme)
    st.markdown(f"""
    <style>
    :root{{--bg:{p["bg"]};--panel:{p["panel"]};--line:{p["line"]};--text:{p["text"]};--muted:{p["muted"]};--cyan:{p["cyan"]};--green:{p["green"]};--input:{p["input"]};--card:{p["card"]};--hero:{p["hero"]};--radius:8px;}}
    html,body,[data-testid="stAppViewContainer"],[data-testid="stApp"]{{background:var(--bg)!important;color:var(--text)!important;font-family:Inter,Segoe UI,system-ui,sans-serif;}}
    [data-testid="stAppViewContainer"]:before{{content:"";position:fixed;inset:0;z-index:-2;background:radial-gradient(circle at 12% 18%,rgba(37,208,255,.18),transparent 28%),radial-gradient(circle at 88% 8%,rgba(51,240,120,.14),transparent 25%),linear-gradient(135deg,var(--bg),var(--panel) 52%,var(--bg));}}
    [data-testid="stAppViewContainer"]:after{{content:"";position:fixed;inset:-30%;z-index:-1;background:linear-gradient(115deg,transparent 0 42%,rgba(255,255,255,.045) 43% 45%,transparent 46% 100%);animation:sweep 9s linear infinite;}}
    @keyframes sweep{{from{{transform:translateX(-28%)}}to{{transform:translateX(28%)}}}}
    #MainMenu,footer,[data-testid="stToolbar"],[data-testid="stDecoration"]{{display:none!important}}
    .block-container{{max-width:100%!important;padding:1rem 1.25rem 2.5rem!important}}
    h1,h2,h3,p,label,span{{color:var(--text)!important}}
    .stTabs [role="tablist"]{{display:grid!important;grid-template-columns:repeat(3,minmax(0,1fr));gap:.55rem;border:0!important}}
    .stTabs button[role="tab"]{{height:48px!important;min-width:0!important;border-radius:var(--radius)!important;background:var(--card)!important;border:1px solid var(--line)!important;color:var(--muted)!important;overflow:hidden!important;white-space:nowrap!important}}
    .stTabs button[aria-selected="true"]{{color:var(--text)!important;border-color:var(--cyan)!important;box-shadow:0 0 24px rgba(37,208,255,.18)!important}}
    .hero{{position:relative;overflow:hidden;border:1px solid rgba(37,208,255,.35);border-radius:var(--radius);padding:2rem;min-height:210px;background:var(--hero),repeating-linear-gradient(90deg,rgba(255,255,255,.04) 0 1px,transparent 1px 70px);box-shadow:0 24px 60px rgba(0,0,0,.18)}}
    .hero:after{{content:"SEA";position:absolute;right:2rem;bottom:-1.4rem;font-size:8rem;font-weight:950;color:rgba(255,255,255,.09);letter-spacing:.06em}}
    .kicker{{color:#d8fff0!important;font-weight:900;letter-spacing:.16em;text-transform:uppercase;font-size:.78rem}}
    .hero h1{{font-size:clamp(2.6rem,5vw,5.2rem);line-height:.95;margin:.35rem 0;color:#fff!important}}
    .hero p{{max-width:830px;color:#e6f6fb!important}}
    .metric-grid,.context-grid{{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:.8rem;margin:1rem 0}}
    .metric,.context-card,.tile,.today,.schedule{{background:var(--card);border:1px solid var(--line);border-radius:var(--radius);box-shadow:0 18px 40px rgba(0,0,0,.16)}}
    .metric{{padding:1rem}}.metric b{{font-size:2rem;color:var(--green)}}.metric div{{color:var(--muted);font-size:.78rem;text-transform:uppercase;font-weight:800;letter-spacing:.1em}}
    .context-card{{padding:1rem;min-height:126px}}.context-card b{{display:block;font-size:.96rem;margin:.25rem 0}}.context-card p{{color:var(--muted)!important;font-size:.84rem;line-height:1.4;margin:0}}
    .context-chip{{display:inline-grid;place-items:center;width:34px;height:34px;border-radius:50%;background:linear-gradient(135deg,var(--cyan),var(--green));color:#06111b!important;font-weight:950}}
    .section{{display:flex;align-items:center;justify-content:space-between;gap:1rem;margin:1.4rem 0 .7rem}}.section h2{{margin:0;font-size:1.35rem}}.section p{{margin:0;color:var(--muted)!important;font-size:.9rem}}
    .stTextInput label,.stSelectbox label{{height:22px!important;color:var(--muted)!important;font-weight:850!important}}
    .stTextInput input{{background:var(--input)!important;color:var(--text)!important;-webkit-text-fill-color:var(--text)!important;border:1px solid var(--cyan)!important;border-radius:var(--radius)!important;height:44px!important}}
    .stTextInput input::placeholder{{color:var(--muted)!important}}
    .stSelectbox [data-baseweb="select"]>div{{background:var(--input)!important;border:1px solid var(--line)!important;border-radius:var(--radius)!important;min-height:44px!important}}
    .stSelectbox [data-baseweb="select"] span{{color:var(--text)!important}}
    .result-count{{color:var(--muted)!important;margin:.45rem 0 0;font-size:.86rem}}.program-row{{margin-bottom:.95rem}}
    .tile{{height:218px;padding:1rem;position:relative;overflow:hidden;border-top:3px solid var(--cyan);transition:transform .18s ease,box-shadow .18s ease}}
    .tile:hover{{transform:translateY(-4px);box-shadow:0 22px 50px rgba(37,208,255,.14)}}.tile.active{{border-color:var(--green);border-top-color:var(--green)}}
    .sport-mark{{width:44px;height:44px;border-radius:50%;display:grid;place-items:center;background:linear-gradient(135deg,var(--cyan),var(--green));color:#03111b!important;font-weight:950}}
    .tile-title{{font-weight:900;font-size:1.04rem;line-height:1.15;height:2.45rem;margin:.75rem 0 .4rem;overflow:hidden}}.meta{{color:var(--muted)!important;font-size:.82rem}}
    .badges{{display:flex;flex-wrap:wrap;gap:.3rem;margin-top:.65rem}}.badge{{border:1px solid var(--line);border-radius:999px;padding:.14rem .48rem;color:var(--text)!important;background:rgba(0,0,0,.06);font-size:.7rem;font-weight:850}}.badge.green{{color:var(--green)!important}}.badge.cyan{{color:var(--cyan)!important}}
    .today{{height:142px;padding:.9rem;border-left:4px solid var(--green)}}.today-time{{color:var(--green)!important;font-weight:950}}.today-name{{font-weight:900;margin:.25rem 0}}.today-center,.time{{color:var(--muted)!important;font-size:.84rem}}
    .center{{font-size:1.05rem;font-weight:950;color:var(--cyan)!important;margin:1rem 0 .5rem}}.schedule{{display:grid;grid-template-columns:92px minmax(0,1fr) 110px;gap:.7rem;align-items:center;padding:.8rem 1rem;margin-bottom:.5rem}}
    .day{{font-weight:950;color:var(--green)!important}}.program{{font-weight:900;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}}
    .empty,.detail-note{{border:1px dashed var(--line);border-radius:var(--radius);padding:1rem;color:var(--muted)!important;background:rgba(37,208,255,.08)}}
    .stButton>button{{height:38px!important;border-radius:var(--radius)!important;background:var(--input)!important;border:1px solid var(--line)!important;color:var(--text)!important;font-weight:850!important;overflow:hidden!important;white-space:nowrap!important}}
    .stButton>button:hover{{border-color:var(--green)!important;color:var(--green)!important}}
    @media(max-width:1100px){{.metric-grid,.context-grid{{grid-template-columns:repeat(2,1fr)}}}}@media(max-width:900px){{.schedule{{grid-template-columns:1fr}}.hero:after{{display:none}}}}
    </style>
    """, unsafe_allow_html=True)


def section(title: str, subtitle: str = "") -> None:
    st.markdown(f'<div class="section"><h2>{escape(title)}</h2><p>{escape(subtitle)}</p></div>', unsafe_allow_html=True)


def render_hero(df: pd.DataFrame) -> None:
    st.markdown(f'<div class="hero"><div class="kicker">Seattle drop-in sports finder</div><h1>Play Seattle tonight.</h1><p>Use this dashboard to find drop-in community sports without digging through individual center pages. Compare {len(df)} weekly sessions across {df["center"].nunique()} Seattle community centers, see what is happening today, and narrow the schedule by program, location, day, or start time.</p></div>', unsafe_allow_html=True)


def render_context() -> None:
    st.markdown("""
    <div class="context-grid">
        <div class="context-card"><span class="context-chip">01</span><b>Find open sessions fast</b><p>Search across program names and community centers from one place.</p></div>
        <div class="context-card"><span class="context-chip">02</span><b>Plan around today</b><p>See today's available drop-in options sorted by start time.</p></div>
        <div class="context-card"><span class="context-chip">03</span><b>Compare programs</b><p>Use tiles to scan session counts, centers, days, and first start times.</p></div>
        <div class="context-card"><span class="context-chip">04</span><b>Use Schedule for details</b><p>The Schedule tab shows the complete filtered timetable, grouped by center. If you select a program tile, it narrows to that program.</p></div>
    </div>
    """, unsafe_allow_html=True)


def render_metrics(df: pd.DataFrame) -> None:
    today = datetime.now().strftime("%A")
    values = [(len(df), "sessions"), (df["program"].nunique(), "programs"), (df["center"].nunique(), "centers"), (int((df["day_of_week"] == today).sum()), "today")]
    st.markdown('<div class="metric-grid">' + ''.join(f'<div class="metric"><b>{v}</b><div>{label}</div></div>' for v, label in values) + '</div>', unsafe_allow_html=True)


def clear_filters(key_prefix: str) -> None:
    for key, value in FILTER_DEFAULTS.items():
        st.session_state[f"{key_prefix}_{key}"] = value
    st.session_state.selected_program = None


def has_active_filters(key_prefix: str) -> bool:
    return any(st.session_state.get(f"{key_prefix}_{key}", value) != value for key, value in FILTER_DEFAULTS.items())


def filtered_data(df: pd.DataFrame, key_prefix: str, title: str = "Explore") -> tuple[pd.DataFrame, str]:
    section(title, "Filter by text, program, center, day, and sort order.")
    c1, c2, c3, c4, c5 = st.columns([1.45, 1, 1, .8, 1])
    with c1:
        q = st.text_input("Search programs or centers", placeholder="Basketball, Ballard, Friday", key=f"{key_prefix}_search")
    with c2:
        program = st.selectbox("Program", ["All Programs"] + sorted(df["program"].unique()), key=f"{key_prefix}_program")
    with c3:
        center = st.selectbox("Center", ["All Centers"] + sorted(df["center"].unique()), key=f"{key_prefix}_center")
    with c4:
        day = st.selectbox("Day", ["All Days"] + [d for d in DAY_ORDER if d in set(df["day_of_week"])], key=f"{key_prefix}_day")
    with c5:
        sort = st.selectbox("Sort", ["Day then time", "Earliest first", "Latest first"], key=f"{key_prefix}_sort")

    # 🔥 LOCAL FILTERING (keeps lat/lon)
    out = df.copy()

    if program != "All Programs":
        out = out[out["program"] == program]

    if center != "All Centers":
        out = out[out["center"] == center]

    if day != "All Days":
        out = out[out["day_of_week"] == day]

    # still keep search client-side (fast + flexible)
    if q.strip():
        out = out[out["_search_blob"].str.contains(q.strip().lower(), na=False)]
    if sort == "Earliest first":
        out = out.sort_values(["_time_order", "_day_order", "program"])
    elif sort == "Latest first":
        out = out.sort_values(["_time_order", "_day_order", "program"], ascending=[False, True, True])
    else:
        out = out.sort_values(["_day_order", "_time_order", "program"])

    left, right = st.columns([1, .22])
    with left:
        st.markdown(f'<div class="result-count">Showing {len(out)} of {len(df)} sessions</div>', unsafe_allow_html=True)
    with right:
        if has_active_filters(key_prefix):
            st.button(
                "Clear filters",
                key=f"{key_prefix}_clear",
                use_container_width=True,
                on_click=clear_filters,
                args=(key_prefix,),
            )
    if out.empty:
        st.markdown('<div class="empty">No sessions match these filters. Clear filters or broaden your search to see programs again.</div>', unsafe_allow_html=True)
    return out, sort


def render_today(df: pd.DataFrame) -> None:
    today = datetime.now().strftime("%A")
    today_df = df[df["day_of_week"] == today].sort_values(["_time_order", "program"]).head(6)
    section("Today's Programs", f"{today}, sorted by time")
    if today_df.empty:
        st.markdown('<div class="empty">No matching programs today. Clear filters or try another day in the Schedule tab.</div>', unsafe_allow_html=True)
        return
    cols = st.columns(3)
    for i, (_, r) in enumerate(today_df.iterrows()):
        with cols[i % 3]:
            st.markdown(f'<div class="today"><div class="today-time">{fmt_time(r.start_time)} - {fmt_time(r.end_time)}</div><div class="today-name"><span class="badge cyan">{sport_badge(r.program)}</span> {escape(r.program)}</div><div class="today-center">{escape(r.center)}</div><div class="badges"><span class="badge green">{age_label(r.age_min)}</span></div></div>', unsafe_allow_html=True)


def render_tiles(df: pd.DataFrame) -> str | None:
    section("Program Tiles", "Click View details to focus the schedule on one program.")
    programs = sorted(df["program"].unique())
    if not programs:
        st.markdown('<div class="empty">No programs match your filters. Use Clear filters above to reset the view.</div>', unsafe_allow_html=True)
        return None
    clicked = None
    selected = st.session_state.get("selected_program")
    for row_start in range(0, len(programs), 5):
        cols = st.columns(5)
        for offset, program in enumerate(programs[row_start:row_start + 5]):
            p_df = df[df["program"] == program]
            active = " active" if selected == program else ""
            days = ''.join(f'<span class="badge cyan">{d[:3]}</span>' for d in sorted(set(p_df.day_of_week), key=lambda d: DAY_TO_NUM.get(d, 99))[:5])
            with cols[offset]:
                first = p_df.sort_values("_time_order").iloc[0].start_time
                st.markdown(f'<div class="tile{active}"><div class="sport-mark">{sport_badge(program)}</div><div class="tile-title">{escape(program)}</div><div class="meta">{len(p_df)} sessions | {p_df.center.nunique()} centers</div><div class="meta">First: {fmt_time(first)}</div><div class="badges">{days}</div></div>', unsafe_allow_html=True)
                if st.button("Selected" if selected == program else f"View details", key=f"tile_{program}", use_container_width=True):
                    clicked = program
    return clicked


def render_schedule(df: pd.DataFrame, title_prefix: str | None = None) -> None:
    selected = st.session_state.get("selected_program")
    view = df[df["program"] == selected] if selected else df
    title = f"{selected} Schedule" if selected else f"{title_prefix or 'Full'} Schedule"
    section(title, "This tab shows the timetable grouped by community center. It updates when filters or program details are selected.")
    if selected:
        st.markdown(f'<div class="detail-note">Viewing details for <b>{escape(selected)}</b>. Clear the selected tile or filters to return to the full schedule.</div>', unsafe_allow_html=True)
    if view.empty:
        st.markdown('<div class="empty">No schedule records are available for this selection. Clear filters or choose another program.</div>', unsafe_allow_html=True)
        return
    for center, cdf in view.sort_values(["center", "_day_order", "_time_order"]).groupby("center"):
        st.markdown(f'<div class="center">{escape(center)}</div>', unsafe_allow_html=True)
        for _, r in cdf.iterrows():
            st.markdown(f'<div class="schedule"><div class="day">{escape(r.day_of_week[:3])}</div><div><div class="program"><span class="badge cyan">{sport_badge(r.program)}</span> {escape(r.program)}</div><div class="time">{fmt_time(r.start_time)} - {fmt_time(r.end_time)}</div></div><div><span class="badge green">{age_label(r.age_min)}</span></div></div>', unsafe_allow_html=True)

import pydeck as pdk

def render_simple_map(df):
    st.subheader("📍 Community Centers")

    map_df = df[["center", "lat", "lon"]].dropna().drop_duplicates()

    if map_df.empty:
        st.warning("No location data available.")
        return

    layer = pdk.Layer(
        "ScatterplotLayer",
        data=map_df,
        get_position='[lon, lat]',
        get_radius=120,
        get_fill_color='[0, 200, 255, 180]',
        pickable=True,
    )

    tooltip = {
        "html": "<b>{center}</b>",
        "style": {"backgroundColor": "#111", "color": "white"}
    }

    view_state = pdk.ViewState(
        latitude=map_df["lat"].mean(),
        longitude=map_df["lon"].mean(),
        zoom=11,
    )

    st.pydeck_chart(pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip=tooltip
    ))

def render_insights_map(df):
    st.subheader("📊 Program Density Map")

    if st.session_state.get("selected_program"):
        df = df[df["program"] == st.session_state["selected_program"]]

    map_df = (
        df.groupby(["center", "lat", "lon"])
        .size()
        .reset_index(name="count")
    )

    map_df = map_df.dropna()

    if map_df.empty:
        st.warning("No location data available.")
        return

    # scale down to avoid clustering
    map_df["radius"] = map_df["count"] * 40

    map_df["color"] = map_df["count"].apply(
        lambda x: [50, min(255, x * 40), 200, 180]
    )

    layer = pdk.Layer(
        "ScatterplotLayer",
        data=map_df,
        get_position='[lon, lat]',
        get_radius="radius",
        get_fill_color="color",
        pickable=True,
        auto_highlight=True,
    )

    tooltip = {
        "html": "<b>{center}</b><br/>Programs: {count}",
        "style": {"backgroundColor": "#111", "color": "white"}
    }

    view_state = pdk.ViewState(
        latitude=map_df["lat"].mean(),
        longitude=map_df["lon"].mean(),
        zoom=11,
    )

    st.pydeck_chart(pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip=tooltip
    ))

def main() -> None:

    theme = st.sidebar.radio("Theme", ["Dark", "Light"], horizontal=True, key="theme")
    inject_css(theme)
    st.session_state.setdefault("selected_program", None)
    try:
        start = time.time()

        placeholder = st.empty()

        with placeholder.container():
            with st.spinner("Loading..."):
                render_skeleton()
                df = fetch_programs()

        # 🔥 enforce minimum skeleton time (0.6s)
        elapsed = time.time() - start
        if elapsed < 0.6:
            time.sleep(0.6 - elapsed)

        placeholder.empty()
    except Exception as exc:
        st.error(f"Could not load {API_BASE}/programs: {exc}")
        st.stop()
    if df.empty:
        st.info("No program records returned.")
        st.stop()

    # Normalize program data
    df["center"] = df["center"].str.strip().str.lower()

    # Load centers
    centers_df = load_centers()

    if not centers_df.empty:
        centers_df = normalize_centers(centers_df)

        # Merge
        df = df.merge(
            centers_df,
            on="center",
            how="left"
        )

    render_hero(df)
    render_context()
    render_metrics(df)

    tab1, tab2, tab3 = st.tabs(["Explore", "Today", "Schedule"])
    with tab1:
        filtered, _ = filtered_data(df, "explore", "Search Programs")

        # 🔥 ADD MAP HERE
        map_mode = st.radio(
            "🗺️ Map View",
            ["Simple", "Insights"],
            horizontal=True
        )
        if map_mode == "Simple":
            st.caption("Explore community centers by location.")
            render_simple_map(filtered)
        else:
            st.caption("Larger and brighter markers indicate more programs.")
            render_insights_map(filtered)

        render_today(filtered)
        clicked = render_tiles(filtered)

        if clicked:
            st.session_state.selected_program = None if st.session_state.selected_program == clicked else clicked
            st.rerun()

        if st.session_state.selected_program:
            render_schedule(filtered, "Selected Program")
    with tab2:
        filtered, _ = filtered_data(df, "today", "Filter Today's View")
        render_today(filtered)
    with tab3:
        st.markdown('<div class="detail-note">Use Schedule when you want the full timetable after filtering. It groups sessions by center, shows day/time/age details, and respects any program tile selected in Explore.</div>', unsafe_allow_html=True)
        filtered, _ = filtered_data(df, "schedule", "Filter Schedule")
        render_schedule(filtered)


if __name__ == "__main__":
    main()

import os
from datetime import datetime
from urllib.parse import urlparse

import pandas as pd
import streamlit as st

# Uses the non-browser scraper we added for Streamlit Cloud
from csv_scraper.scraper_serp import scrape_serp_jobs

# ---------- Page setup ----------
st.set_page_config(page_title="NZ Job Scraper for Clients", page_icon="🧑‍💼", layout="wide")

# Subtle styling to match your “client” look
st.markdown("""
<style>
/* headings */
h1, h2, h3 { letter-spacing: 0.2px; }
/* softer widgets */
.stButton>button {
    border-radius: 12px; padding: 0.7rem 1.1rem; font-weight: 600;
}
.stCheckbox>label { font-size: 1rem; }
.small-note { color:#6b7280; font-size:0.9rem; }
</style>
""", unsafe_allow_html=True)

# ---------- Header ----------
st.markdown("## 🧑‍💼 NZ Job Scraper for Clients")

# ---------- Top form ----------
colA, colB = st.columns([1,2], vertical_alignment="center")
with colA:
    client = st.selectbox("Choose a client", ["Oppong Millicent", "New Client…"])
    if client == "New Client…":
        client = st.text_input("Client name", value="", placeholder="Type a name…")

with colB:
    query = st.text_input("Job Search Query", value="Teacher jobs new zealand", placeholder="e.g., Boilermaker jobs New Zealand")

st.markdown("### Choose job boards to search:")

cb1 = st.checkbox("Seek", value=True)
cb2 = st.checkbox("TradeMe", value=True)
cb3 = st.checkbox("Indeed", value=True)
cb4 = st.checkbox("Glassdoor", value=False, disabled=True)  # placeholder for later

run = st.button("🔍 Run Scraper", type="primary")

st.markdown("<div class='small-note'>Tip: Results come from Google Jobs (SerpAPI). Board filters are applied by the link’s domain.</div>", unsafe_allow_html=True)

# ---------- Helpers ----------
def normalize_filename(s: str) -> str:
    s = (s or "").strip().replace(" ", "_")
    cleaned = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in s)
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    return cleaned[:120] or "results"

# Map each “board” to link-domain patterns we accept
BOARD_PATTERNS = {
    "Seek": ["seek.co.nz", "seek.com", "seek.com.au"],
    "TradeMe": ["trademe.co.nz"],
    "Indeed": ["indeed.com", "indeed.co.nz"],
    "Glassdoor": ["glassdoor.com", "glassdoor.co.nz"],  # for future use
}

def host_matches(host: str, patterns: list[str]) -> bool:
    host = host.lower()
    return any(p in host for p in patterns)

def filter_by_boards(df: pd.DataFrame, use_seek: bool, use_trademe: bool, use_indeed: bool):
    # If all unchecked, return empty
    if not any([use_seek, use_trademe, use_indeed]):
        return df.iloc[0:0]

    selected = []
    if use_seek: selected += BOARD_PATTERNS["Seek"]
    if use_trademe: selected += BOARD_PATTERNS["TradeMe"]
    if use_indeed: selected += BOARD_PATTERNS["Indeed"]

    if df.empty: return df
    df = df.copy()
    df["__host"] = df["Application Weblink"].fillna("").apply(lambda u: urlparse(u).netloc)
    df = df[df["__host"].apply(lambda h: host_matches(h, selected))]
    df = df.drop(columns=["__host"])
    return df

# ---------- Main action ----------
if run:
    if not query.strip():
        st.error("Please enter a search query.")
        st.stop()

    # Check for API key early to show a nice message
    if not (os.getenv("GOOGLE_API_KEY") or os.getenv("SERPAPI_API_KEY")):
        st.error("Missing SerpAPI key. In Streamlit Cloud: Settings → Secrets → add GOOGLE_API_KEY = your_serpapi_key_here")
        st.stop()

    # Visual status blocks (like “Searching Seek…” in your screenshot)
    with st.status("🔎 Searching Google Jobs…", expanded=True) as status:
        st.write(f"Client: **{client or '—'}**")
        st.write(f"Query: **{query}**")

        # Pull pages of results (10 per page). You can tweak num_pages if you want a control here.
        try:
            raw_jobs = scrape_serp_jobs(query, location="New Zealand", num_pages=3)
        except Exception as e:
            st.error(f"Scrape failed: {e}")
            st.stop()

        st.write(f"Fetched **{len(raw_jobs)}** jobs from Google Jobs index.")
        df_all = pd.DataFrame(raw_jobs)

        # Apply board filters
        df_filtered = filter_by_boards(df_all, cb1, cb2, cb3)

        st.write("Applied board filters:",
                 ("Seek " if cb1 else "") + ("TradeMe " if cb2 else "") + ("Indeed" if cb3 else ""))
        status.update(label="✨ Finished search", state="complete")

    # ---------- Results ----------
    if df_filtered.empty:
        st.warning("No jobs matched the selected boards. Try widening the query or enabling more boards.")
    else:
        st.success(f"Found {len(df_filtered):,} matching jobs.")
        # Nice column ordering when available
        cols = [c for c in ["Source", "Position Title", "Company Name", "Location", "Posted", "Application Weblink"] if c in df_filtered.columns]
        rest = [c for c in df_filtered.columns if c not in cols]
        df_view = df_filtered[cols + rest]
        st.dataframe(df_view, use_container_width=True)

        # Download
        ts = datetime.now().strftime("%Y%m%d-%H%M")
        base = normalize_filename(query)
        fname = f"{base}_{ts}.csv"
        st.download_button("⬇️ Download CSV", df_view.to_csv(index=False).encode("utf-8-sig"),
                           file_name=fname, mime="text/csv")

# Footer
st.markdown("---")
st.caption("Runs on Streamlit Community Cloud using SerpAPI (no browser needed).")

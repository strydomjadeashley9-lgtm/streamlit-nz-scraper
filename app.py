# app.py — NZ Job Scraper for Clients (Seek-only)

import os
from datetime import datetime
from urllib.parse import urlparse

import requests
import pandas as pd
import streamlit as st

# Uses the scraper you already added in your repo:
# csv_scraper/scraper_serp.py must define scrape_serp_jobs(query, location, num_pages)
from csv_scraper.scraper_serp import scrape_serp_jobs

# ---------------- Page setup ----------------
st.set_page_config(page_title="NZ Job Scraper for Clients", page_icon="🧑‍💼", layout="wide")
st.markdown("## 🧑‍💼 NZ Job Scraper for Clients")

# ---------------- Secrets helpers ----------------
def get_secret(name: str, default: str | None = None) -> str | None:
    """Prefer Streamlit secrets; fallback to env vars."""
    return str(st.secrets.get(name, os.getenv(name, default)) or "")

GOOGLE_API_KEY         = get_secret("GOOGLE_API_KEY")         # SerpAPI key
AIRTABLE_API_KEY       = get_secret("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID       = get_secret("AIRTABLE_BASE_ID")
AIRTABLE_CLIENTS_TABLE = get_secret("AIRTABLE_CLIENTS_TABLE", "Job Seekers")
AIRTABLE_VIEW          = get_secret("AIRTABLE_VIEW", "Grid view")
AIRTABLE_CLIENT_FIELD  = get_secret("AIRTABLE_CLIENT_FIELD", "Full Name")

# Tiny debug line so you can verify the source at a glance (safe to keep)
st.caption(f"Clients from Airtable → **{AIRTABLE_CLIENTS_TABLE} / {AIRTABLE_VIEW} / {AIRTABLE_CLIENT_FIELD}**")

# ---------------- Airtable fetch ----------------
def fetch_airtable_clients() -> list[str]:
    """Fetch client names from Airtable `Job Seekers` table using the `Full Name` field."""
    if not (AIRTABLE_API_KEY and AIRTABLE_BASE_ID and AIRTABLE_CLIENTS_TABLE):
        return []

    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{requests.utils.quote(AIRTABLE_CLIENTS_TABLE)}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}
    params = {}
    if AIRTABLE_VIEW:
        params["view"] = AIRTABLE_VIEW

    names, offset = [], None
    while True:
        if offset:
            params["offset"] = offset
        r = requests.get(url, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()

        for rec in data.get("records", []):
            f = rec.get("fields", {})
            # Use the exact field from Secrets, default "Full Name"
            name = f.get(AIRTABLE_CLIENT_FIELD)
            if isinstance(name, str) and name.strip():
                names.append(name.strip())

        offset = data.get("offset")
        if not offset:
            break

    # De-dupe (keep order)
    seen, ordered = set(), []
    for n in names:
        if n not in seen:
            seen.add(n); ordered.append(n)
    return ordered

# ---------------- Seek-only helpers ----------------
SEEK_DOMAINS = ["seek.co.nz", "seek.com", "seek.com.au", "nz.seek.co.nz"]

def is_seek_link(url: str) -> bool:
    host = urlparse(url or "").netloc.lower()
    return any(d in host for d in SEEK_DOMAINS)

def normalize_filename(s: str) -> str:
    s = (s or "").strip().replace(" ", "_")
    cleaned = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in s)
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    return cleaned[:120] or "results"

# ---------------- UI ----------------
colA, colB = st.columns([1, 2])

with colA:
    try:
        clients = fetch_airtable_clients()
    except Exception as e:
        st.error(f"Error fetching clients from Airtable: {e}")
        clients = []

    client = (
        st.selectbox("Choose a client", clients)
        if clients else
        st.text_input("Client name", placeholder="Type a name…")
    )

with colB:
    query = st.text_input("Job Search Query", value="Teacher jobs new zealand")

st.markdown("### Job board")
st.checkbox("Seek", value=True, disabled=True, help="Only Seek is enabled.")
run = st.button("🔍 Run Scraper", type="primary")

# ---------------- Run ----------------
if run:
    # Basic validations
    if not client.strip():
        st.error("Please choose or enter a client name.")
        st.stop()
    if not query.strip():
        st.error("Please enter a search query.")
        st.stop()
    if not GOOGLE_API_KEY:
        st.error("Missing GOOGLE_API_KEY in Secrets.")
        st.stop()

    with st.status("🔎 Searching Seek via Google Jobs (SerpAPI)…", expanded=True) as status:
        st.write(f"Client: **{client}**")
        st.write(f"Query: **{query}**")

        # Use your existing scraper; get more pages by raising num_pages
        raw_jobs = scrape_serp_jobs(query, location="New Zealand", num_pages=3)
        seek_jobs = [j for j in raw_jobs if is_seek_link(j.get("Application Weblink", ""))]

        st.write(f"Fetched {len(raw_jobs)} jobs; {len(seek_jobs)} are from Seek.")
        status.update(label="✨ Finished search", state="complete")

    df = pd.DataFrame(seek_jobs)
    if df.empty:
        st.warning("No Seek jobs found for this query.")
    else:
        st.success(f"Found {len(df):,} Seek jobs.")
        st.dataframe(df, use_container_width=True)

        ts = datetime.now().strftime("%Y%m%d-%H%M")
        fname = f"{normalize_filename(client)}_{normalize_filename(query)}_{ts}_SEEK.csv"
        st.download_button(
            "⬇️ Download CSV",
            df.to_csv(index=False).encode("utf-8-sig"),
            file_name=fname,
            mime="text/csv",
        )

st.markdown("---")
st.caption("Client list comes from Airtable. Jobs are filtered to Seek only.")

import os
from datetime import datetime
from urllib.parse import urlparse

import requests
import pandas as pd
import streamlit as st

from csv_scraper.scraper_serp import scrape_serp_jobs

st.set_page_config(page_title="NZ Job Scraper for Clients", page_icon="🧑‍💼", layout="wide")
st.markdown("## 🧑‍💼 NZ Job Scraper for Clients")

# ---------------- Secrets ----------------
def get_secret(name: str, default: str | None = None) -> str | None:
    if name in st.secrets:
        return str(st.secrets[name])
    return os.getenv(name, default)

GOOGLE_API_KEY       = get_secret("GOOGLE_API_KEY")
AIRTABLE_API_KEY     = get_secret("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID     = get_secret("AIRTABLE_BASE_ID")
AIRTABLE_CLIENTS_TABLE = get_secret("AIRTABLE_CLIENTS_TABLE", "Job Seekers")

# ---------------- Airtable fetch ----------------
def fetch_airtable_clients() -> list[str]:
    if not (AIRTABLE_API_KEY and AIRTABLE_BASE_ID and AIRTABLE_CLIENTS_TABLE):
        return []
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{requests.utils.quote(AIRTABLE_CLIENTS_TABLE)}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}
    clients = []
    offset = None
    while True:
        params = {}
        if offset:
            params["offset"] = offset
        r = requests.get(url, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        payload = r.json()
        for rec in payload.get("records", []):
            fields = rec.get("fields", {})
            name = fields.get("Name") or fields.get("Client") or fields.get("Full Name")
            if isinstance(name, str) and name.strip():
                clients.append(name.strip())
        offset = payload.get("offset")
        if not offset:
            break
    return sorted(set(clients))

# ---------------- Seek filter ----------------
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
colA, colB = st.columns([1, 2], vertical_alignment="center")

with colA:
    try:
        clients = fetch_airtable_clients()
    except Exception as e:
        st.error(f"Error fetching clients from Airtable: {e}")
        clients = []

    if clients:
        client = st.selectbox("Choose a client", clients, index=0, key="client_select")
    else:
        client = st.text_input("Client name", value="", placeholder="Type a name…")

with colB:
    query = st.text_input("Job Search Query", value="Teacher jobs new zealand")

st.markdown("### Job board")
st.checkbox("Seek", value=True, disabled=True, help="Only Seek is enabled at the moment.")
run = st.button("🔍 Run Scraper", type="primary")

# ---------------- Run ----------------
if run:
    if not client.strip():
        st.error("Please choose or enter a client name.")
        st.stop()
    if not query.strip():
        st.error("Please enter a search query.")
        st.stop()
    if not GOOGLE_API_KEY:
        st.error("Missing GOOGLE_API_KEY in Secrets")
        st.stop()

    with st.status("🔎 Searching Seek via Google Jobs (SerpAPI)…", expanded=True) as status:
        st.write(f"Client: **{client}**")
        st.write(f"Query: **{query}**")

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
        st.download_button("⬇️ Download CSV",
                           df.to_csv(index=False).encode("utf-8-sig"),
                           file_name=fname,
                           mime="text/csv")

st.markdown("---")
st.caption("Client list comes from Airtable. Jobs from Google Jobs (Seek only).")

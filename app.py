import os
import requests
import pandas as pd
import streamlit as st
from urllib.parse import urlparse
from datetime import datetime

from csv_scraper.scraper_serp import scrape_serp_jobs

st.set_page_config(page_title="NZ Job Scraper for Clients", page_icon="🧑‍💼", layout="wide")
st.markdown("## 🧑‍💼 NZ Job Scraper for Clients")

# ---------------- Secrets ----------------
AIRTABLE_API_KEY = st.secrets["AIRTABLE_API_KEY"]
AIRTABLE_BASE_ID = st.secrets["AIRTABLE_BASE_ID"]
AIRTABLE_CLIENTS_TABLE = st.secrets["AIRTABLE_CLIENTS_TABLE"]
GOOGLE_API_KEY = st.secrets["GOOGLE_API_KEY"]

# ---------------- Airtable fetch ----------------
def fetch_clients():
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
        data = r.json()
        for rec in data.get("records", []):
            fields = rec.get("fields", {})
            if "Name" in fields and fields["Name"].strip():
                clients.append(fields["Name"].strip())
        offset = data.get("offset")
        if not offset:
            break
    return sorted(set(clients))

# ---------------- Helpers ----------------
SEEK_DOMAINS = ["seek.co.nz", "seek.com", "seek.com.au", "nz.seek.co.nz"]

def is_seek(url: str) -> bool:
    host = urlparse(url or "").netloc.lower()
    return any(d in host for d in SEEK_DOMAINS)

def normalize_filename(s: str) -> str:
    s = (s or "").strip().replace(" ", "_")
    return "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in s)[:120] or "results"

# ---------------- UI ----------------
col1, col2 = st.columns([1, 2])
with col1:
    clients = fetch_clients()
    client = st.selectbox("Choose a client", clients)

with col2:
    query = st.text_input("Job Search Query", value="Teacher jobs new zealand")

st.markdown("### Job board")
st.checkbox("Seek", value=True, disabled=True, help="Only Seek is enabled.")
run = st.button("🔍 Run Scraper", type="primary")

# ---------------- Run ----------------
if run:
    with st.status("🔎 Searching Seek…", expanded=True) as status:
        st.write(f"Client: **{client}**")
        st.write(f"Query: **{query}**")

        jobs = scrape_serp_jobs(query, location="New Zealand", num_pages=3)
        seek_jobs = [j for j in jobs if is_seek(j.get("Application Weblink", ""))]

        st.write(f"Fetched {len(jobs)} jobs; {len(seek_jobs)} from Seek.")
        status.update(label="✨ Finished search", state="complete")

    df = pd.DataFrame(seek_jobs)
    if df.empty:
        st.warning("No Seek jobs found for this query.")
    else:
        st.success(f"Found {len(df)} Seek jobs.")
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
st.caption("Clients loaded from Airtable → Job Seekers table. Jobs from Seek (via SerpAPI).")

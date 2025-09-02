# app.py — NZ Job Scraper for Clients (Seek-only) with Occupation from Airtable

import os
from datetime import datetime
from urllib.parse import urlparse

import requests
import pandas as pd
import streamlit as st

from csv_scraper.scraper_serp import scrape_serp_jobs

st.set_page_config(page_title="NZ Job Scraper for Clients", page_icon="🧑‍💼", layout="wide")
st.markdown("## 🧑‍💼 NZ Job Scraper for Clients")

# ---------------- Secrets helpers ----------------
def get_secret(name: str, default: str | None = None) -> str | None:
    return str(st.secrets.get(name, os.getenv(name, default)) or "")

# SerpAPI key (either name is fine)
SERPAPI_KEY            = get_secret("SERPAPI_KEY") or get_secret("GOOGLE_API_KEY")

AIRTABLE_API_KEY       = get_secret("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID       = get_secret("AIRTABLE_BASE_ID")
AIRTABLE_CLIENTS_TABLE = get_secret("AIRTABLE_CLIENTS_TABLE", "Job Seekers")
AIRTABLE_VIEW          = get_secret("AIRTABLE_VIEW", "Grid view")
AIRTABLE_CLIENT_FIELD  = get_secret("AIRTABLE_CLIENT_FIELD", "Full Name")
AIRTABLE_CLIENT_PROF   = get_secret("AIRTABLE_CLIENT_PROF_FIELD", "Profession")

st.caption(
    f"Clients from Airtable → **{AIRTABLE_CLIENTS_TABLE} / {AIRTABLE_VIEW} / "
    f"{AIRTABLE_CLIENT_FIELD} + {AIRTABLE_CLIENT_PROF}**"
)

# ---------------- Airtable fetch (name + profession) ----------------
def fetch_airtable_clients() -> list[dict]:
    if not (AIRTABLE_API_KEY and AIRTABLE_BASE_ID and AIRTABLE_CLIENTS_TABLE):
        return []
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{requests.utils.quote(AIRTABLE_CLIENTS_TABLE)}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}
    params = {"view": AIRTABLE_VIEW} if AIRTABLE_VIEW else {}

    rows, offset = [], None
    while True:
        if offset:
            params["offset"] = offset
        r = requests.get(url, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        payload = r.json()
        for rec in payload.get("records", []):
            f = rec.get("fields", {})
            name = (f.get(AIRTABLE_CLIENT_FIELD) or "").strip()
            prof = (f.get(AIRTABLE_CLIENT_PROF) or "").strip()
            if name:
                rows.append({"name": name, "profession": prof})
        offset = payload.get("offset")
        if not offset:
            break

    seen, out = set(), []
    for row in rows:
        if row["name"] not in seen:
            seen.add(row["name"]); out.append(row)
    return out

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
    clients = []
    try:
        clients = fetch_airtable_clients()
    except Exception as e:
        st.error(f"Airtable error: {e}")

    names = [c["name"] for c in clients]
    selected_name = (
        st.selectbox("Choose a client", names, index=0) if names
        else st.text_input("Client name", placeholder="Type a name…")
    )
    selected = next((c for c in clients if c["name"] == selected_name), None) if names else None
    occupation = (selected or {}).get("profession", "").strip()

with colB:
    default_query = f"{occupation} jobs new zealand" if occupation else "Teacher jobs new zealand"
    query = st.text_input("Job Search Query", value=default_query)

if occupation:
    st.markdown(f"**Occupation:** {occupation}")

st.markdown("### Job board")
st.checkbox("Seek", value=True, disabled=True, help="Only Seek is enabled.")
run = st.button("🔍 Run Scraper", type="primary")

# ---------------- Run ----------------
if run:
    client_name = selected_name
    if not client_name.strip():
        st.error("Please choose or enter a client name."); st.stop()
    if not query.strip():
        st.error("Please enter a search query."); st.stop()
    if not SERPAPI_KEY:
        st.error("Missing SERPAPI_KEY (or GOOGLE_API_KEY) in Secrets."); st.stop()

    with st.status("🔎 Searching Seek via Google Jobs (SerpAPI)…", expanded=True) as status:
        st.write(f"Client: **{client_name}**")
        if occupation:
            st.write(f"Occupation: **{occupation}**")
        st.write(f"Query: **{query}**")

        try:
            raw_jobs = scrape_serp_jobs(query, location="New Zealand", num_pages=3, api_key=SERPAPI_KEY)
        except Exception as e:
            st.error(f"Search failed: {e}")
            st.stop()

        seek_jobs = [j for j in raw_jobs if is_seek_link(j.get("Application Weblink", ""))]

        st.write(f"Fetched {len(raw_jobs)} jobs; {len(seek_jobs)} from Seek.")
        status.update(label="✨ Finished search", state="complete")

    df = pd.DataFrame(seek_jobs)
    if df.empty:
        st.warning("No Seek jobs found for this query.")
    else:
        df.insert(0, "Client", client_name)
        if occupation:
            df.insert(1, "Occupation", occupation)

        st.success(f"Found {len(df):,} Seek jobs.")
        st.dataframe(df, use_container_width=True)

        ts = datetime.now().strftime("%Y%m%d-%H%M")
        fname = f"{normalize_filename(client_name)}_{normalize_filename(query)}_{ts}_SEEK.csv"
        st.download_button(
            "⬇️ Download CSV",
            df.to_csv(index=False).encode("utf-8-sig"),
            file_name=fname,
            mime="text/csv",
        )

st.markdown("---")
st.caption("Client & Occupation pulled from Airtable. Jobs filtered to Seek only.")

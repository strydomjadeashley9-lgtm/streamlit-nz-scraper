# NZ Job Scraper (Streamlit Cloud) — simple, stable version
# Uses SerpAPI Google Jobs with next_page_token pagination.
# No Airtable, no filters — just return jobs and let you download a CSV.

import os
import time
from datetime import datetime
from urllib.parse import urlparse

import pandas as pd
import streamlit as st
from serpapi import GoogleSearch

st.set_page_config(page_title="NZ Job Scraper", page_icon="🔎", layout="wide")
st.title("🔎 NZ Job Scraper (Streamlit Cloud)")
st.caption("Runs on Streamlit Cloud using SerpAPI — no local installs needed.")

# --- Helpers -----------------------------------------------------------------
def get_key() -> str | None:
    # Works with either name; use SERPAPI_KEY if you can
    return str(st.secrets.get("SERPAPI_KEY", st.secrets.get("GOOGLE_API_KEY", "")) or "")

def first_nonempty(*vals):
    for v in vals:
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""

def normalize_filename(s: str) -> str:
    s = (s or "").strip().replace(" ", "_")
    cleaned = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in s)
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    return cleaned[:120] or "results"

def scrape_jobs(query: str, location: str = "New Zealand", pages: int = 2, api_key: str | None = None):
    """Return a list of job dicts from Google Jobs via SerpAPI."""
    key = api_key or get_key()
    if not key:
        raise RuntimeError("Missing SERPAPI_KEY (or GOOGLE_API_KEY) in Streamlit secrets.")

    rows, seen_links = [], set()
    next_token = None
    pages_left = max(1, int(pages))

    while pages_left > 0:
        params = {
            "engine": "google_jobs",
            "q": query,
            "location": location,
            "api_key": key,
        }
        if next_token:
            params["next_page_token"] = next_token

        data = GoogleSearch(params).get_dict()
        if "error" in data:
            # show a clear, user-facing error
            raise RuntimeError(f"SerpAPI error: {data['error']}")

        jobs = data.get("jobs_results", []) or []
        for j in jobs:
            title = j.get("title", "")
            company = j.get("company_name", "")
            loc = j.get("location", "")
            exts = j.get("detected_extensions", {}) or {}
            posted = first_nonempty(exts.get("posted_at"), exts.get("posted"))

            link = first_nonempty(
                j.get("apply_link"),
                (j.get("apply_options") or [{}])[0].get("link"),
                j.get("link"),
            )
            if link and link in seen_links:
                continue
            if link:
                seen_links.add(link)

            # Try to label the source site
            host = urlparse(link or "").netloc.lower()
            source = first_nonempty(j.get("via"), j.get("source"), host, "Web")

            rows.append({
                "Source": source,
                "Position Title": title,
                "Company Name": company,
                "Location": loc,
                "Posted": posted,
                "Application Weblink": link,
            })

        pages_left -= 1
        next_token = (
            data.get("serpapi_pagination", {}).get("next_page_token")
            or data.get("search_metadata", {}).get("next_page_token")
        )
        if not next_token:
            break

        time.sleep(0.6)  # gentle rate limit

    return rows

# --- UI ----------------------------------------------------------------------
col1, col2 = st.columns([2, 1])
with col1:
    query = st.text_input("Search query", value="mechanical design engineer new zealand")
with col2:
    pages = st.slider("Pages (×10 results)", min_value=1, max_value=5, value=2)

run = st.button("🚀 Scrape Now", type="primary")

if run:
    if not query.strip():
        st.error("Please enter a search query.")
        st.stop()

    with st.status("Searching Google Jobs…", expanded=True) as status:
        st.write(f"Query: **{query}**, Pages: **{pages}**")
        try:
            jobs = scrape_jobs(query=query, location="New Zealand", pages=pages)
        except Exception as e:
            st.error(str(e))
            st.stop()

        status.update(label="✅ Done", state="complete")

    df = pd.DataFrame(jobs)
    if df.empty:
        st.warning("No jobs found for this query. Try a broader phrase.")
    else:
        st.success(f"Found {len(df):,} jobs.")
        st.dataframe(df, use_container_width=True)

        ts = datetime.now().strftime("%Y%m%d-%H%M")
        fname = f"{normalize_filename(query)}_{ts}.csv"
        st.download_button(
            "⬇️ Download CSV",
            df.to_csv(index=False).encode("utf-8-sig"),
            file_name=fname,
            mime="text/csv",
        )

st.markdown("---")
st.caption("Simple, reliable version. To re-add clients/Airtable later, we can layer it back once you're ready.")

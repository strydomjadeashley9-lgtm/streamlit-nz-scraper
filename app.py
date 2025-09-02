import os
from datetime import datetime
from pathlib import Path
import pandas as pd
import streamlit as st
from csv_scraper.scraper_serp import scrape_serp_jobs

st.set_page_config(page_title="NZ Job Scraper", page_icon="🔎", layout="wide")
st.title("🔎 NZ Job Scraper (Streamlit Cloud)")
st.caption("Runs on Streamlit Community Cloud using SerpAPI — no local installs needed.")

with st.sidebar:
    st.header("Options")
    default_query = "mechanical design engineer New Zealand"
    query = st.text_input("Search query", value=default_query)
    pages = st.slider("Pages (x10 results)", min_value=1, max_value=5, value=2)
    run_btn = st.button("🚀 Scrape Now", type="primary")

def normalize_filename(s: str) -> str:
    s = s.strip().replace(" ", "_")
    cleaned = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in s)
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    return cleaned[:120] or "results"

if run_btn:
    if not query.strip():
        st.error("Please enter a search query.")
        st.stop()

    with st.spinner("Scraping Google Jobs via SerpAPI…"):
        jobs = scrape_serp_jobs(query, location="New Zealand", num_pages=pages)

    if not jobs:
        st.warning("No jobs found. Try another query.")
    else:
        df = pd.DataFrame(jobs)
        st.success(f"Found {len(df):,} jobs.")
        st.dataframe(df, use_container_width=True)

        # Offer CSV download
        ts = datetime.now().strftime("%Y%m%d-%H%M")
        fname = f"{normalize_filename(query)}_{ts}.csv"
        csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
        st.download_button("⬇️ Download CSV", data=csv_bytes, file_name=fname, mime="text/csv")

st.markdown("---")
st.caption("Tip: once this is live, share the public URL with your partner. They can run searches and download CSVs.")

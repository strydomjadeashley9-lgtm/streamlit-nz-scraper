import os
import time
import logging
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus

import requests
import pandas as pd
import streamlit as st
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("job-scraper")

st.set_page_config(page_title="NZ Job Scraper for Clients", page_icon="🧑‍💼", layout="wide")
st.title("🧑‍💼 NZ Job Scraper for Clients")

# --- Secrets helpers ---
def get_secret(name: str, default: str = "") -> str:
    return str(st.secrets.get(name, os.getenv(name, default)) or "")

AIRTABLE_API_KEY = get_secret("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = get_secret("AIRTABLE_BASE_ID")
AIRTABLE_CLIENTS_TABLE = get_secret("AIRTABLE_CLIENTS_TABLE", "Job Seekers")
AIRTABLE_VIEW = get_secret("AIRTABLE_VIEW", "Grid view")
AIRTABLE_CLIENT_FIELD = get_secret("AIRTABLE_CLIENT_FIELD", "Full Name")
AIRTABLE_CLIENT_PROF_FIELD = get_secret("AIRTABLE_CLIENT_PROF_FIELD", "Profession")

# --- Airtable fetch (your working version) ---
@st.cache_data(ttl=300)
def fetch_clients():
    if not all([AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_CLIENTS_TABLE]):
        return []
    
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{requests.utils.quote(AIRTABLE_CLIENTS_TABLE)}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}
    params = {}
    if AIRTABLE_VIEW:
        params["view"] = AIRTABLE_VIEW
    
    clients = []
    offset = None
    
    try:
        while True:
            if offset:
                params["offset"] = offset
                
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            for record in data.get("records", []):
                fields = record.get("fields", {})
                name = fields.get(AIRTABLE_CLIENT_FIELD, "").strip()
                profession = fields.get(AIRTABLE_CLIENT_PROF_FIELD, "").strip()
                
                if name:
                    clients.append({"name": name, "profession": profession})
            
            offset = data.get("offset")
            if not offset:
                break
                
        return clients
    except Exception as e:
        st.error(f"Error fetching clients from Airtable: {e}")
        return []

# --- Direct Seek scraping (adapted from your working scraper_seek.py) ---
def scrape_seek_direct(query: str):
    """Direct HTTP scraping of Seek (no Selenium needed for basic scraping)"""
    if not query:
        return []
    
    jobs = []
    
    try:
        # Use the same URL pattern as your working scraper
        url = f"https://www.seek.co.nz/{quote_plus(query)}-jobs"
        logger.info(f"🔎 Seek URL: {url}")
        
        # Headers to mimic a real browser
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        }
        
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        # Use your exact parsing logic from scraper_seek.py
        soup = BeautifulSoup(response.content, "html.parser")
        
        # Your exact CSS selectors
        cards = soup.select("[data-automation='searchResults'] article, article") or soup.select("a[href*='/job/'], [data-automation='job-card']")
        
        for card in cards:
            # Your exact parsing logic
            title_el = card.select_one("[data-automation='jobTitle']") or card.select_one("a[data-automation]")
            title = (title_el.get_text(strip=True) if title_el else "") or (card.get("aria-label") or "").strip()
            
            link = ""
            link_el = card.select_one("a[href*='/job/']")
            if link_el and link_el.get("href"):
                href = link_el["href"]
                link = href if href.startswith("http") else f"https://www.seek.co.nz{href}"
            
            company_el = card.select_one("[data-automation='jobCompany']")
            company = company_el.get_text(strip=True) if company_el else ""
            
            loc_el = card.select_one("[data-automation='jobLocation']")
            location = loc_el.get_text(strip=True) if loc_el else ""
            
            posted_el = card.select_one("[data-automation='jobListingDate']")
            posted = posted_el.get_text(strip=True) if posted_el else ""
            
            if title and link:
                jobs.append({
                    "Source": "Seek",
                    "Position Title": title,
                    "Company Name": company,
                    "Location": location,
                    "Posted": posted,
                    "Application Weblink": link,
                })
        
        logger.info(f"✅ Found {len(jobs)} jobs from Seek")
        return jobs
        
    except requests.RequestException as e:
        logger.error(f"Error scraping Seek: {e}")
        st.warning(f"Could not scrape Seek directly: {e}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        st.warning(f"Unexpected error scraping Seek: {e}")
        return []

# --- TradeMe scraping (simplified version) ---
def scrape_trademe_direct(query: str):
    """Direct HTTP scraping of TradeMe Jobs"""
    if not query:
        return []
    
    jobs = []
    try:
        # TradeMe jobs URL pattern
        url = f"https://www.trademe.co.nz/jobs/search?search_string={quote_plus(query)}"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        }
        
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, "html.parser")
        
        # TradeMe specific selectors (you'd need to inspect their page)
        job_cards = soup.select("div[data-testid*='listing']") or soup.select(".listing-item")
        
        for card in job_cards:
            title_el = card.select_one("h3 a, .listing-title a")
            title = title_el.get_text(strip=True) if title_el else ""
            
            link = ""
            if title_el and title_el.get("href"):
                href = title_el["href"]
                link = href if href.startswith("http") else f"https://www.trademe.co.nz{href}"
            
            # Extract other fields as available
            company = ""  # TradeMe might not always show company
            location_el = card.select_one(".location, .listing-region")
            location = location_el.get_text(strip=True) if location_el else ""
            
            if title and link:
                jobs.append({
                    "Source": "TradeMe",
                    "Position Title": title,
                    "Company Name": company,
                    "Location": location,
                    "Posted": "",
                    "Application Weblink": link,
                })
        
        logger.info(f"✅ Found {len(jobs)} jobs from TradeMe")
        return jobs
        
    except Exception as e:
        logger.error(f"Error scraping TradeMe: {e}")
        st.warning(f"Could not scrape TradeMe: {e}")
        return []

# Helper function
def normalize_filename(text: str) -> str:
    text = text.strip().replace(" ", "_")
    return "".join(c for c in text if c.isalnum() or c in ("_", "-"))[:50]

# --- UI (your working version) ---
col1, col2 = st.columns([1, 2])

with col1:
    # Client selection (your working approach)
    clients_data = fetch_clients()
    
    if clients_data:
        client_names = [client["name"] for client in clients_data]
        selected_client_name = st.selectbox("Choose a client", client_names)
        
        # Find selected client's data
        selected_client = next(
            (client for client in clients_data if client["name"] == selected_client_name), 
            {"name": selected_client_name, "profession": ""}
        )
        
        profession = selected_client.get("profession", "")
        if profession:
            st.markdown(f"**Occupation:** {profession}")
    else:
        st.info("No clients found in Airtable. Using manual input.")
        selected_client_name = st.text_input("Client name", placeholder="Enter client name...")
        profession = st.text_input("Profession (optional)", placeholder="e.g., Teacher")
        selected_client = {"name": selected_client_name, "profession": profession}

with col2:
    # Query input with auto-fill (your working approach)
    default_query = f"{profession} jobs new zealand" if profession else "jobs new zealand"
    query = st.text_input("Job search query", value=default_query)

# Job boards selection (your working UI)
st.markdown("### Choose job boards to search:")
use_seek = st.checkbox("Seek", value=True)
use_trademe = st.checkbox("TradeMe", value=True)
use_indeed = st.checkbox("Indeed", value=False, disabled=True, help="Indeed blocks scraping")
use_glassdoor = st.checkbox("Glassdoor", value=False, disabled=True, help="Not yet implemented")

# Run button
run_scraper = st.button("🔍 Run Scraper", type="primary")

# Execute scraping (your working approach)
if run_scraper:
    if not selected_client["name"].strip():
        st.error("Please select or enter a client name.")
        st.stop()
    
    if not query.strip():
        st.error("Please enter a search query.")
        st.stop()
    
    # Show search progress
    with st.status("🔎 Scraping job boards...", expanded=True) as status:
        st.write(f"**Client:** {selected_client['name']}")
        if selected_client.get('profession'):
            st.write(f"**Profession:** {selected_client['profession']}")
        st.write(f"**Query:** {query}")
        
        all_jobs = []
        
        # Scrape each selected board
        if use_seek:
            st.write("🔎 Searching Seek...")
            seek_jobs = scrape_seek_direct(query)
            all_jobs.extend(seek_jobs)
            
        if use_trademe:
            st.write("🔎 Searching TradeMe...")
            trademe_jobs = scrape_trademe_direct(query)
            all_jobs.extend(trademe_jobs)
        
        status.update(label="✅ Scraping completed!", state="complete")
    
    if not all_jobs:
        st.warning("No jobs found. This could be because:")
        st.write("• The job sites changed their HTML structure")
        st.write("• They're blocking requests from cloud servers")  
        st.write("• Try simpler search terms")
        
        # Debug info
        with st.expander("Debug: What we tried"):
            if use_seek:
                st.write(f"Seek URL: https://www.seek.co.nz/{quote_plus(query)}-jobs")
            if use_trademe:
                st.write(f"TradeMe URL: https://www.trademe.co.nz/jobs/search?search_string={quote_plus(query)}")
    else:
        # Create DataFrame (your working approach)
        df = pd.DataFrame(all_jobs)
        
        # Add client information to the data
        df.insert(0, "Client", selected_client["name"])
        if selected_client.get("profession"):
            df.insert(1, "Client Profession", selected_client["profession"])
        
        st.success(f"Found {len(all_jobs)} jobs!")
        
        # Display results
        st.dataframe(df, use_container_width=True)
        
        # Download button (your working approach)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        client_safe = normalize_filename(selected_client["name"])
        filename = f"{client_safe}_jobs_{timestamp}.csv"
        
        csv_data = df.to_csv(index=False)
        st.download_button(
            label="⬇️ Download CSV",
            data=csv_data,
            file_name=filename,
            mime="text/csv"
        )
        
        # Show summary by source
        st.markdown("### Results by Source")
        source_counts = df["Source"].value_counts()
        for source, count in source_counts.items():
            st.write(f"• {source}: {count} jobs")

# Footer
st.markdown("---")
st.caption("Direct scraping approach based on your working local scraper • Client data from Airtable")

import os
import time
from datetime import datetime
from urllib.parse import urlparse

import requests
import pandas as pd
import streamlit as st
from serpapi import GoogleSearch

# Page config
st.set_page_config(page_title="NZ Job Scraper for Clients", page_icon="🧑‍💼", layout="wide")
st.title("🧑‍💼 NZ Job Scraper for Clients")

# Get secrets
def get_secret(name: str, default: str = "") -> str:
    return str(st.secrets.get(name, os.getenv(name, default)) or "")

SERPAPI_KEY = get_secret("SERPAPI_KEY") or get_secret("GOOGLE_API_KEY")
AIRTABLE_API_KEY = get_secret("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = get_secret("AIRTABLE_BASE_ID")
AIRTABLE_CLIENTS_TABLE = get_secret("AIRTABLE_CLIENTS_TABLE", "Job Seekers")
AIRTABLE_VIEW = get_secret("AIRTABLE_VIEW", "Grid view")
AIRTABLE_CLIENT_FIELD = get_secret("AIRTABLE_CLIENT_FIELD", "Full Name")
AIRTABLE_CLIENT_PROF_FIELD = get_secret("AIRTABLE_CLIENT_PROF_FIELD", "Profession")

# Fetch clients from Airtable
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

# Scrape jobs using SerpAPI
def scrape_jobs(query: str, location: str = "New Zealand", num_pages: int = 2):
    if not SERPAPI_KEY:
        raise ValueError("SERPAPI_KEY is required in Streamlit secrets")
    
    all_jobs = []
    next_token = None
    pages_scraped = 0
    
    while pages_scraped < num_pages:
        params = {
            "engine": "google_jobs",
            "q": query,
            "location": location,
            "api_key": SERPAPI_KEY
        }
        
        if next_token:
            params["next_page_token"] = next_token
        
        try:
            search = GoogleSearch(params)
            results = search.get_dict()
            
            if "error" in results:
                raise ValueError(f"SerpAPI error: {results['error']}")
            
            jobs = results.get("jobs_results", [])
            
            if not jobs:
                break
                
            for job in jobs:
                # Extract job details
                title = job.get("title", "")
                company = job.get("company_name", "")
                job_location = job.get("location", "")
                
                # Get posting date
                extensions = job.get("detected_extensions", {})
                posted = extensions.get("posted_at", "") or extensions.get("posted", "")
                
                # Get application link
                apply_link = ""
                if job.get("apply_link"):
                    apply_link = job["apply_link"]
                elif job.get("apply_options") and len(job["apply_options"]) > 0:
                    apply_link = job["apply_options"][0].get("link", "")
                elif job.get("link"):
                    apply_link = job["link"]
                
                # Determine source
                source = job.get("via", "")
                if not source:
                    # Try to determine from URL
                    if apply_link:
                        host = urlparse(apply_link).netloc.lower()
                        if "seek.co.nz" in host or "seek.com" in host:
                            source = "Seek"
                        elif "trademe.co.nz" in host:
                            source = "Trade Me"
                        elif "indeed" in host:
                            source = "Indeed"
                        elif "jora" in host:
                            source = "Jora"
                        else:
                            source = host or "Web"
                    else:
                        source = "Web"
                
                all_jobs.append({
                    "Source": source,
                    "Position Title": title,
                    "Company Name": company,
                    "Location": job_location,
                    "Posted": posted,
                    "Application Weblink": apply_link
                })
            
            # Get next page token
            serpapi_pagination = results.get("serpapi_pagination", {})
            next_token = serpapi_pagination.get("next_page_token")
            
            if not next_token:
                break
                
            pages_scraped += 1
            time.sleep(0.5)  # Rate limiting
            
        except Exception as e:
            raise ValueError(f"Error scraping jobs: {str(e)}")
    
    return all_jobs

# Helper function
def normalize_filename(text: str) -> str:
    text = text.strip().replace(" ", "_")
    return "".join(c for c in text if c.isalnum() or c in ("_", "-"))[:50]

# Main UI
col1, col2 = st.columns([1, 2])

with col1:
    # Client selection
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
    # Query input with auto-fill
    default_query = f"{profession} jobs new zealand" if profession else "jobs new zealand"
    query = st.text_input("Job search query", value=default_query)

# Additional controls
num_pages = st.slider("Number of pages to scrape", min_value=1, max_value=5, value=2, 
                      help="Each page contains approximately 10 job results")

# Run button
run_scraper = st.button("🔍 Run Scraper", type="primary")

# Execute scraping
if run_scraper:
    if not selected_client["name"].strip():
        st.error("Please select or enter a client name.")
        st.stop()
    
    if not query.strip():
        st.error("Please enter a search query.")
        st.stop()
    
    if not SERPAPI_KEY:
        st.error("SERPAPI_KEY is missing from Streamlit secrets. Please add it in Settings > Secrets.")
        st.stop()
    
    # Show search progress
    with st.status("🔎 Searching for jobs...", expanded=True) as status:
        st.write(f"**Client:** {selected_client['name']}")
        if selected_client.get('profession'):
            st.write(f"**Profession:** {selected_client['profession']}")
        st.write(f"**Query:** {query}")
        st.write(f"**Pages to scrape:** {num_pages}")
        
        try:
            jobs = scrape_jobs(query, num_pages=num_pages)
            status.update(label="✅ Search completed!", state="complete")
        except Exception as e:
            st.error(f"Search failed: {e}")
            st.stop()
    
    if not jobs:
        st.warning("No jobs found for this query. Try adjusting your search terms.")
    else:
        # Create DataFrame
        df = pd.DataFrame(jobs)
        
        # Add client information to the data
        df.insert(0, "Client", selected_client["name"])
        if selected_client.get("profession"):
            df.insert(1, "Client Profession", selected_client["profession"])
        
        st.success(f"Found {len(jobs)} jobs!")
        
        # Display results
        st.dataframe(df, use_container_width=True)
        
        # Download button
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
        
        # Show summary
        st.markdown("### Summary")
        source_counts = df["Source"].value_counts()
        for source, count in source_counts.items():
            st.write(f"• {source}: {count} jobs")

# Footer
st.markdown("---")
st.caption("Powered by SerpAPI • Client data from Airtable")

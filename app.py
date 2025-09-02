import os
import time
from datetime import datetime
from urllib.parse import urlparse

import requests
import pandas as pd
import streamlit as st
from serpapi import GoogleSearch

st.set_page_config(page_title="NZ Job Scraper for Clients", page_icon="🧑‍💼", layout="wide")
st.title("🧑‍💼 NZ Job Scraper for Clients")

# --- Secrets helpers ---
def get_secret(name: str, default: str = "") -> str:
    return str(st.secrets.get(name, os.getenv(name, default)) or "")

SERPAPI_KEY = get_secret("SERPAPI_KEY") or get_secret("GOOGLE_API_KEY")
AIRTABLE_API_KEY = get_secret("AIRTABLE_API_KEY")
AIRTABLE_BASE_ID = get_secret("AIRTABLE_BASE_ID")
AIRTABLE_CLIENTS_TABLE = get_secret("AIRTABLE_CLIENTS_TABLE", "Job Seekers")
AIRTABLE_VIEW = get_secret("AIRTABLE_VIEW", "Grid view")
AIRTABLE_CLIENT_FIELD = get_secret("AIRTABLE_CLIENT_FIELD", "Full Name")
AIRTABLE_CLIENT_PROF_FIELD = get_secret("AIRTABLE_CLIENT_PROF_FIELD", "Profession")

# --- Airtable fetch ---
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

# --- Enhanced job scraping with multiple strategies ---
def scrape_jobs_smart(query: str, location: str = "New Zealand"):
    """
    Multi-strategy job search that tries different approaches to find jobs
    """
    if not SERPAPI_KEY:
        raise ValueError("SERPAPI_KEY is required in Streamlit secrets")
    
    all_jobs = []
    
    # Strategy 1: Direct site searches (most effective)
    site_strategies = [
        f"site:seek.co.nz {query}",
        f"site:trademe.co.nz {query}",
        f"site:indeed.co.nz {query}",
        f"site:jora.co.nz {query}",
    ]
    
    for i, site_query in enumerate(site_strategies):
        site_name = site_query.split()[0].replace("site:", "").replace(".co.nz", "").replace(".com", "").title()
        
        try:
            st.write(f"🔍 Searching {site_name}...")
            
            params = {
                "engine": "google",  # Use regular Google search, not google_jobs
                "q": site_query,
                "location": location,
                "api_key": SERPAPI_KEY,
                "num": 20,  # Get more results
            }
            
            search = GoogleSearch(params)
            results = search.get_dict()
            
            if "error" in results:
                st.write(f"❌ {site_name} error: {results['error']}")
                continue
            
            organic_results = results.get("organic_results", [])
            site_jobs = []
            
            for result in organic_results:
                title = result.get("title", "")
                link = result.get("link", "")
                snippet = result.get("snippet", "")
                
                # Skip non-job results
                if not any(word in title.lower() for word in ["job", "career", "position", "vacancy", "role"]):
                    if not any(word in snippet.lower() for word in ["job", "career", "apply", "position", "vacancy"]):
                        continue
                
                # Extract company and location from snippet/title
                company = ""
                job_location = ""
                
                # Try to extract company from snippet
                if " at " in snippet:
                    company = snippet.split(" at ")[1].split(".")[0].split(",")[0].strip()
                elif " - " in title:
                    parts = title.split(" - ")
                    if len(parts) > 1:
                        company = parts[-1].strip()
                
                # Try to extract location
                if "auckland" in snippet.lower():
                    job_location = "Auckland"
                elif "wellington" in snippet.lower():
                    job_location = "Wellington"
                elif "christchurch" in snippet.lower():
                    job_location = "Christchurch"
                elif "new zealand" in snippet.lower():
                    job_location = "New Zealand"
                
                site_jobs.append({
                    "Source": site_name,
                    "Position Title": title,
                    "Company Name": company,
                    "Location": job_location,
                    "Posted": "",
                    "Application Weblink": link,
                    "Description": snippet[:200] + "..." if len(snippet) > 200 else snippet
                })
            
            all_jobs.extend(site_jobs)
            st.write(f"✅ {site_name}: Found {len(site_jobs)} jobs")
            
            time.sleep(1)  # Rate limiting
            
        except Exception as e:
            st.write(f"❌ {site_name} error: {str(e)}")
            continue
    
    # Strategy 2: Generic job searches if we don't have enough results
    if len(all_jobs) < 10:
        generic_queries = [
            f'"{query}" jobs {location}',
            f'{query} career {location}',
            f'{query} position {location}',
        ]
        
        for generic_query in generic_queries:
            if len(all_jobs) >= 20:  # Stop if we have enough
                break
                
            try:
                st.write(f"🔍 Trying: {generic_query}")
                
                params = {
                    "engine": "google_jobs",  # Try Google Jobs for generic searches
                    "q": generic_query,
                    "location": location,
                    "api_key": SERPAPI_KEY,
                }
                
                search = GoogleSearch(params)
                results = search.get_dict()
                
                if "error" in results:
                    continue
                
                jobs_results = results.get("jobs_results", [])
                
                for job in jobs_results:
                    title = job.get("title", "")
                    company = job.get("company_name", "")
                    job_location = job.get("location", "")
                    
                    extensions = job.get("detected_extensions", {})
                    posted = extensions.get("posted_at", "") or extensions.get("posted", "")
                    
                    apply_link = (job.get("apply_link") or 
                                (job.get("apply_options", [{}])[0].get("link")) or
                                job.get("link", ""))
                    
                    # Determine source from link
                    source = "Web"
                    if apply_link:
                        host = urlparse(apply_link).netloc.lower()
                        if "seek" in host:
                            source = "Seek"
                        elif "trademe" in host:
                            source = "TradeMe"
                        elif "indeed" in host:
                            source = "Indeed"
                        elif "jora" in host:
                            source = "Jora"
                    
                    all_jobs.append({
                        "Source": source,
                        "Position Title": title,
                        "Company Name": company,
                        "Location": job_location,
                        "Posted": posted,
                        "Application Weblink": apply_link,
                        "Description": job.get("description", "")[:200] + "..."
                    })
                
                st.write(f"✅ Generic search: Found {len(jobs_results)} additional jobs")
                time.sleep(1)
                
            except Exception as e:
                st.write(f"❌ Generic search error: {str(e)}")
                continue
    
    # Remove duplicates based on title + company
    unique_jobs = []
    seen = set()
    
    for job in all_jobs:
        key = (job.get("Position Title", "").lower().strip(), 
               job.get("Company Name", "").lower().strip())
        if key not in seen and key != ("", ""):
            seen.add(key)
            unique_jobs.append(job)
    
    return unique_jobs

def normalize_filename(text: str) -> str:
    text = text.strip().replace(" ", "_")
    return "".join(c for c in text if c.isalnum() or c in ("_", "-"))[:50]

# --- UI ---
col1, col2 = st.columns([1, 2])

with col1:
    clients_data = fetch_clients()
    
    if clients_data:
        client_names = [client["name"] for client in clients_data]
        selected_client_name = st.selectbox("Choose a client", client_names)
        
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
    # Smart query building
    if profession:
        # Remove common suffixes that don't help with searching
        clean_profession = profession.replace(" jobs", "").replace(" new zealand", "").strip()
        default_query = clean_profession
    else:
        default_query = "jobs"
    
    query = st.text_input("Job search query", value=default_query,
                         help="Use simple terms like 'teacher' or 'engineer' - avoid 'jobs new zealand'")

# Info box
st.info("💡 **Search Tips:** Use simple job titles (e.g., 'teacher', 'engineer', 'plumber') rather than full phrases. The system will search across multiple NZ job sites.")

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
    with st.status("🔎 Searching job boards...", expanded=True) as status:
        st.write(f"**Client:** {selected_client['name']}")
        if selected_client.get('profession'):
            st.write(f"**Profession:** {selected_client['profession']}")
        st.write(f"**Query:** {query}")
        
        try:
            jobs = scrape_jobs_smart(query)
            status.update(label="✅ Search completed!", state="complete")
        except Exception as e:
            st.error(f"Search failed: {e}")
            st.stop()
    
    if not jobs:
        st.warning("No jobs found for this query. Try:")
        st.write("• Simpler terms: 'teacher' instead of 'primary school teacher'")
        st.write("• Different keywords: 'health safety' instead of 'HSE'")
        st.write("• Broader terms: 'engineer' instead of 'mechanical engineer'")
    else:
        # Create DataFrame
        df = pd.DataFrame(jobs)
        
        # Add client information
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
        st.markdown("### Results Summary")
        source_counts = df["Source"].value_counts()
        for source, count in source_counts.items():
            st.write(f"• {source}: {count} jobs")

# Footer
st.markdown("---")
st.caption("🔄 **Smart Search Strategy:** Uses site-specific searches to bypass anti-bot protection • Client data from Airtable")

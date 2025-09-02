import os, time, requests

SERP_API_KEY = os.getenv("GOOGLE_API_KEY") or os.getenv("SERPAPI_API_KEY")

def scrape_serp_jobs(query: str, location: str = "New Zealand", num_pages: int = 1):
    if not SERP_API_KEY:
        raise RuntimeError("Missing SERP API key. Set GOOGLE_API_KEY (or SERPAPI_API_KEY) in Streamlit Secrets.")
    all_jobs = []
    for i in range(num_pages):
        params = {
            "engine": "google_jobs",
            "q": f"{query} {location}".strip(),
            "hl": "en",
            "start": i * 10,
            "api_key": SERP_API_KEY,
        }
        r = requests.get("https://serpapi.com/search", params=params, timeout=60)
        r.raise_for_status()
        data = r.json()
        for j in data.get("jobs_results", []):
            all_jobs.append({
                "Source": "Google Jobs (SerpAPI)",
                "Position Title": j.get("title",""),
                "Company Name": j.get("company_name",""),
                "Location": j.get("location",""),
                "Posted": j.get("detected_extensions", {}).get("posted_at",""),
                "Application Weblink": (j.get("apply_options") or [{}])[0].get("link") or j.get("link",""),
            })
        time.sleep(0.6)
    return all_jobs

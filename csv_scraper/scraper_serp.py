# csv_scraper/scraper_serp.py
# Google Jobs via SerpAPI (Seek-friendly)
import os
import time
from urllib.parse import urlparse

from serpapi import GoogleSearch


def _first_nonempty(*vals):
    for v in vals:
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def _is_seek(url: str) -> bool:
    host = urlparse(url or "").netloc.lower()
    return any(d in host for d in ("seek.co.nz", "seek.com.au", "seek.com", "nz.seek.co.nz"))


def scrape_serp_jobs(query: str, location: str = "New Zealand", num_pages: int = 3, api_key: str | None = None):
    """
    Returns a list[dict] with keys:
      Source, Position Title, Company Name, Location, Posted, Application Weblink
    """
    key = api_key or os.getenv("SERPAPI_KEY") or os.getenv("GOOGLE_API_KEY")
    if not key:
        raise RuntimeError("SerpAPI key missing. Provide SERPAPI_KEY or GOOGLE_API_KEY.")

    all_rows = []
    for page in range(max(1, int(num_pages))):
        params = {
            "engine": "google_jobs",
            "q": query,
            "location": location,
            "api_key": key,
            "start": page * 10,  # pagination
        }
        data = GoogleSearch(params).get_dict()
        if "error" in data:
            # Bubble up a clean error so Streamlit shows it nicely
            raise RuntimeError(f"SerpAPI error: {data['error']}")

        jobs = data.get("jobs_results", []) or []
        for j in jobs:
            title = j.get("title", "")
            company = j.get("company_name", "")
            loc = j.get("location", "")
            exts = j.get("detected_extensions", {}) or {}
            posted = _first_nonempty(exts.get("posted_at"), exts.get("posted"))

            # Best available link
            apply_link = _first_nonempty(
                j.get("apply_link"),
                (j.get("apply_options") or [{}])[0].get("link"),
                j.get("link"),
            )

            # Tag source heuristically
            src = "Seek" if _is_seek(apply_link) else _first_nonempty(j.get("via"), j.get("source"), "Web")

            row = {
                "Source": src,
                "Position Title": title,
                "Company Name": company,
                "Location": loc,
                "Posted": posted,
                "Application Weblink": apply_link,
            }
            all_rows.append(row)

        # Stop early if the page returned <10 results
        if len(jobs) < 10:
            break

        # Gentle delay (respect rate limits)
        time.sleep(0.7)

    return all_rows

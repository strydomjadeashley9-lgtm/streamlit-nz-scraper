# csv_scraper/scraper_serp.py
# Google Jobs via SerpAPI (Seek-friendly) with next_page_token pagination

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


def scrape_serp_jobs(query: str,
                     location: str = "New Zealand",
                     num_pages: int = 3,
                     api_key: str | None = None) -> list[dict]:
    """
    Returns a list[dict] with keys:
      Source, Position Title, Company Name, Location, Posted, Application Weblink
    Uses SerpAPI google_jobs engine and paginates with next_page_token.
    """
    key = api_key or os.getenv("SERPAPI_KEY") or os.getenv("GOOGLE_API_KEY")
    if not key:
        raise RuntimeError("SerpAPI key missing. Provide SERPAPI_KEY or GOOGLE_API_KEY.")

    rows: list[dict] = []
    next_token = None
    pages_left = max(1, int(num_pages))

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

        # Surface API errors cleanly
        if "error" in data:
            raise RuntimeError(f"SerpAPI error: {data['error']}")

        jobs = data.get("jobs_results", []) or []
        for j in jobs:
            title = j.get("title", "")
            company = j.get("company_name", "")
            loc = j.get("location", "")
            exts = j.get("detected_extensions", {}) or {}
            posted = _first_nonempty(exts.get("posted_at"), exts.get("posted"))

            apply_link = _first_nonempty(
                j.get("apply_link"),
                (j.get("apply_options") or [{}])[0].get("link"),
                j.get("link"),
            )
            src = "Seek" if _is_seek(apply_link) else _first_nonempty(j.get("via"), j.get("source"), "Web")

            rows.append({
                "Source": src,
                "Position Title": title,
                "Company Name": company,
                "Location": loc,
                "Posted": posted,
                "Application Weblink": apply_link,
            })

        # Prepare next page
        # Token can appear under different keys depending on engine version
        next_token = (
            data.get("serpapi_pagination", {}).get("next_page_token")
            or data.get("search_metadata", {}).get("next_page_token")
        )

        pages_left -= 1
        if not next_token:  # no more pages available
            break

        time.sleep(0.6)  # gentle rate limiting

    return rows

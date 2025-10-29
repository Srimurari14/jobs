#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Scrapes jobs from Greenhouse, Lever, SmartRecruiters (and optional Workday),
filters for relevance, saves to SQLite (jobs.sqlite). CSV export is handled
by the CI workflow.

Install (locally):
  pip install requests beautifulsoup4 playwright pandas tenacity
  playwright install

Run (example):
  python jobscraper.py \
    --greenhouse stripe snowflake \
    --lever databricks \
    --smartrecruiters nvidia \
    --locations "United States" Remote "Washington, DC"
"""

import re, json, time, hashlib, sqlite3, argparse
from dataclasses import dataclass
from typing import List, Dict, Any, Iterable, Optional
import requests
from tenacity import retry, wait_exponential, stop_after_attempt

DB_PATH = "jobs.sqlite"

# ------------------ DB ------------------

def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
          id TEXT PRIMARY KEY,
          source TEXT,
          company TEXT,
          title TEXT,
          location TEXT,
          department TEXT,
          url TEXT,
          posted_at TEXT,
          raw JSON
        );
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_company ON jobs(company);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_posted ON jobs(posted_at);")

def upsert_job(job: Dict[str, Any]):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
        INSERT INTO jobs (id, source, company, title, location, department, url, posted_at, raw)
        VALUES (:id, :source, :company, :title, :location, :department, :url, :posted_at, :raw)
        ON CONFLICT(id) DO UPDATE SET
          title=excluded.title,
          location=excluded.location,
          department=excluded.department,
          url=excluded.url,
          posted_at=excluded.posted_at,
          raw=excluded.raw;
        """, job)

# ------------------ Relevance ------------------

@dataclass
class RelevanceRule:
    include_titles: List[str] = None
    include_keywords: List[str] = None
    exclude_titles: List[str] = None
    exclude_keywords: List[str] = None
    locations_allow: List[str] = None

def contains_any(text: str, patterns: List[str]) -> bool:
    return any(re.search(rf"\b{re.escape(p)}\b", text, flags=re.I) for p in (patterns or []))

def is_relevant(job: Dict[str, Any], rule: RelevanceRule) -> bool:
    title = (job.get("title") or "")
    desc  = (job.get("description") or "")
    loc   = (job.get("location") or "")
    blob  = f"{title}\n{desc}"

    if rule.locations_allow and not (contains_any(loc, rule.locations_allow) or contains_any(blob, rule.locations_allow)):
        return False
    if rule.include_titles and not contains_any(title, rule.include_titles):
        return False
    if rule.include_keywords and not contains_any(blob, rule.include_keywords):
        return False
    if rule.exclude_titles and contains_any(title, rule.exclude_titles):
        return False
    if rule.exclude_keywords and contains_any(blob, rule.exclude_keywords):
        return False
    return True

# ------------------ HTTP ------------------

def job_id(*parts) -> str:
    h = hashlib.sha256("||".join([str(p) for p in parts if p]).encode()).hexdigest()
    return h[:32]

@retry(wait=wait_exponential(multiplier=1, min=1, max=10), stop=stop_after_attempt(3))
def get_json(url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    r = requests.get(url, params=params, timeout=25, headers={"User-Agent": "JobScraper/1.0"})
    r.raise_for_status()
    return r.json()

# ------------------ Adapters ------------------

def scrape_greenhouse(company_slug: str) -> Iterable[Dict[str, Any]]:
    data = get_json(f"https://boards-api.greenhouse.io/v1/boards/{company_slug}/jobs")
    for j in data.get("jobs", []):
        yield {
            "source": "greenhouse",
            "company": company_slug,
            "title": j.get("title"),
            "location": (j.get("location") or {}).get("name", ""),
            "department": (j.get("departments") or [{}])[0].get("name", ""),
            "url": j.get("absolute_url") or j.get("hosted_url"),
            "posted_at": j.get("updated_at") or j.get("created_at"),
            "description": "",
            "raw": j
        }

def scrape_lever(company_slug: str) -> Iterable[Dict[str, Any]]:
    data = get_json(f"https://api.lever.co/v0/postings/{company_slug}", params={"mode": "json"})
    for j in data:
        cats = j.get("categories") or {}
        loc = cats.get("location") or ""
        dept = cats.get("team") or ""
        lists = j.get("lists") or []
        description = ""
        if lists and isinstance(lists[0], dict):
            description = lists[0].get("content", "") or ""
        yield {
            "source": "lever",
            "company": company_slug,
            "title": j.get("text"),
            "location": loc,
            "department": dept,
            "url": j.get("hostedUrl") or j.get("applyUrl"),
            "posted_at": j.get("createdAt"),
            "description": description,
            "raw": j
        }

def scrape_smartrecruiters(company_slug: str, limit: int = 1000) -> Iterable[Dict[str, Any]]:
    base = f"https://api.smartrecruiters.com/v1/companies/{company_slug}/postings"
    start = 0
    while True:
        data = get_json(base, params={"limit": 100, "offset": start})
        postings = data.get("content", [])
        for p in postings:
            yield {
                "source": "smartrecruiters",
                "company": company_slug,
                "title": p.get("name"),
                "location": ((p.get("location") or {}).get("city") or "").strip(),
                "department": (p.get("department") or {}).get("label", ""),
                "url": (p.get("ref") or {}).get("jobAdUrl"),
                "posted_at": p.get("releasedDate"),
                "description": "",
                "raw": p
            }
        start += len(postings)
        if not postings or start >= limit:
            break

def scrape_workday(job_site_url: str, default_company: str = "") -> Iterable[Dict[str, Any]]:
    # Best-effort heuristic for Workday sites (requires playwright)
    from playwright.sync_api import sync_playwright
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(job_site_url, wait_until="networkidle")
        html = page.content()
        browser.close()
    m = re.search(r'window\.__INITIAL_STATE__\s*=\s*(\{.*?\})\s*;', html, flags=re.S)
    if not m:
        return []
    try:
        state = json.loads(m.group(1))
    except Exception:
        return []
    jobs = state.get("jobPostings", []) or state.get("search", {}).get("results", [])
    for j in jobs:
        title = j.get("title") or j.get("displayTitle")
        url = j.get("externalUrl") or j.get("externalPath") or job_site_url
        if isinstance(j.get("locations"), list) and j["locations"]:
            loc = j["locations"][0].get("city", "") or j["locations"][0].get("name", "")
        else:
            loc = j.get("location", "") or ""
        yield {
            "source": "workday",
            "company": default_company,
            "title": title,
            "location": loc,
            "department": j.get("department", "") or "",
            "url": url if (isinstance(url, str) and url.startswith("http")) else job_site_url,
            "posted_at": j.get("postedOn") or j.get("postingDate"),
            "description": "",
            "raw": j
        }

# ------------------ Orchestrator ------------------

def normalize_and_save(rows: Iterable[Dict[str, Any]], relevance: RelevanceRule, default_company: str = "") -> int:
    saved = 0
    for r in rows or []:
        r["company"] = r.get("company") or default_company
        rid = job_id(r.get("source"), r.get("company"), r.get("title"), r.get("url"), r.get("location"))
        if not is_relevant(r, relevance):
            continue
        job = {
            "id": rid,
            "source": r.get("source", ""),
            "company": r.get("company", "").strip(),
            "title": (r.get("title") or "").strip(),
            "location": (r.get("location") or "").strip(),
            "department": (r.get("department") or "").strip(),
            "url": r.get("url", ""),
            "posted_at": str(r.get("posted_at", "")),
            "raw": json.dumps(r, ensure_ascii=False)
        }
        upsert_job(job)
        saved += 1
    return saved

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--greenhouse", nargs="*", default=[], help="Company slugs for Greenhouse")
    parser.add_argument("--lever", nargs="*", default=[], help="Company slugs for Lever")
    parser.add_argument("--smartrecruiters", nargs="*", default=[], help="Company slugs for SmartRecruiters")
    parser.add_argument("--workday", nargs="*", default=[], help="Public job site URLs (Workday)")
    parser.add_argument("--workday-company", nargs="*", default=[], help="Labels for each --workday URL (same order)")
    parser.add_argument("--include-titles", nargs="*", default=["data","engineer","scientist","analyst","sde","software","ml","ai"])
    parser.add_argument("--include-keywords", nargs="*", default=["python","sql","pyspark","spark","aws","gcp","azure","tableau","dbt","airflow"])
    parser.add_argument("--exclude-titles", nargs="*", default=["director","vp","principal","staff","distinguished"])
    parser.add_argument("--exclude-keywords", nargs="*", default=["unpaid","volunteer"])
    parser.add_argument("--locations", nargs="*", default=["United States","Remote","Hybrid","Washington, DC","New York","California","Texas"])
    args = parser.parse_args()

    init_db()
    rule = RelevanceRule(
        include_titles=args.include_titles,
        include_keywords=args.include_keywords,
        exclude_titles=args.exclude_titles,
        exclude_keywords=args.exclude_keywords,
        locations_allow=args.locations
    )

    total = 0
    for slug in args.greenhouse:
        total += normalize_and_save(scrape_greenhouse(slug), rule, default_company=slug); time.sleep(0.4)
    for slug in args.lever:
        total += normalize_and_save(scrape_lever(slug), rule, default_company=slug); time.sleep(0.4)
    for slug in args.smartrecruiters:
        total += normalize_and_save(scrape_smartrecruiters(slug), rule, default_company=slug); time.sleep(0.4)
    for i, url in enumerate(args.workday):
        label = args.workday_company[i] if i < len(args.workday_company) else ""
        total += normalize_and_save(scrape_workday(url, default_company=label), rule, default_company=label); time.sleep(0.8)

    print(f"Saved {total} relevant postings to {DB_PATH}")

if __name__ == "__main__":
    main()

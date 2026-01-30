"""
MBFC Scraper - Scrapes media source bias and credibility data
from mediabiasfactcheck.com across all categories.
"""

import csv
import json
import os
import random
import re
import subprocess
import time

from bs4 import BeautifulSoup

BASE_URL = "https://mediabiasfactcheck.com"

CATEGORIES = {
    "Left": "/left/",
    "Right": "/right/",
}

FIELDNAMES = [
    "name",
    "url",
    "category",
    "source_url",
    "bias_rating",
    "bias_score",
    "factual_reporting",
    "factual_score",
    "country",
    "freedom_rating",
    "media_type",
    "traffic",
    "credibility",
]

OUTPUT_CSV = "mbfc_data.csv"
OUTPUT_JSON = "mbfc_data.json"
REQUEST_DELAY = 1  # base seconds between requests (jitter added automatically)
MAX_PER_CATEGORY = None  # set to a number to limit for testing


def delay():
    """Sleep with random jitter to avoid rate limiting."""
    time.sleep(REQUEST_DELAY + random.uniform(0.5, 2.0))


def fetch_url(url, retries=3):
    """Fetch a URL using curl (avoids TLS fingerprinting issues with Python requests)."""
    for attempt in range(retries):
        result = subprocess.run(
            ["curl", "-s", "-w", "\n%{http_code}", url],
            capture_output=True, text=True, timeout=30,
        )
        output = result.stdout
        # Last line is the HTTP status code
        lines = output.rsplit("\n", 1)
        if len(lines) == 2:
            html, status = lines[0], lines[1].strip()
        else:
            html, status = output, "0"

        if status == "429":
            wait = 30 * (attempt + 1)
            print(f"    Rate limited (429), waiting {wait}s...")
            time.sleep(wait)
            continue

        if not status.startswith("2"):
            raise RuntimeError(f"HTTP {status} for {url}")

        return html

    raise RuntimeError(f"Failed after {retries} retries for {url}")


def get_soup(url, retries=3):
    """Fetch a URL and return a BeautifulSoup object."""
    html = fetch_url(url, retries=retries)
    try:
        return BeautifulSoup(html, "lxml")
    except Exception:
        return BeautifulSoup(html, "html.parser")


def scrape_category(category_name, path):
    """Scrape a category page and return a list of (name, url) tuples."""
    url = BASE_URL + path
    print(f"  Fetching category: {category_name} ({url})")
    soup = get_soup(url)
    table = soup.find("table", {"id": "mbfc-table"})
    if not table:
        print(f"  WARNING: No mbfc-table found for {category_name}")
        return []

    sources = []
    for a in table.find_all("a"):
        name = a.get_text(strip=True)
        link = a.get("href")
        if name and link:
            sources.append((name, link))

    print(f"  Found {len(sources)} sources in {category_name}")
    return sources


def parse_rating_field(text):
    """Parse a rating field like 'LEFT (-5.3)' into (label, score)."""
    match = re.match(r"^(.+?)\s*\(([-\d.]+)\)\s*$", text.strip())
    if match:
        return match.group(1).strip(), float(match.group(2))
    return text.strip(), None


def scrape_source(url):
    """Scrape an individual MBFC source page for bias/credibility data."""
    soup = get_soup(url)
    content = soup.get_text(separator="\n")

    data = {}

    # Extract the actual source website URL from the "Source:" link
    source_link = None
    for a in soup.find_all("a"):
        href = a.get("href", "")
        text = a.get_text(strip=True)
        if href and text and href == text and not href.startswith(BASE_URL):
            source_link = href
            break
    if not source_link:
        match = re.search(r"Source:\s*(https?://[^\s]+)", content)
        if match:
            source_link = match.group(1).strip()
    data["source_url"] = source_link

    # Patterns: "Label: Value" or "Label:Value"
    patterns = {
        "bias_rating_raw": r"Bias Rating:\s*(.+)",
        "factual_reporting_raw": r"Factual Reporting:\s*(.+)",
        "country": r"Country:\s*(.+)",
        "freedom_rating": r"Country Freedom Rating:\s*(.+)",
        "media_type": r"Media Type:\s*(.+)",
        "traffic": r"Traffic/Popularity:\s*(.+)",
        "credibility": r"MBFC Credibility Rating:\s*(.+)",
    }

    for key, pattern in patterns.items():
        match = re.search(pattern, content, re.IGNORECASE)
        if match:
            data[key] = match.group(1).strip()

    # Parse bias rating into label + score
    if "bias_rating_raw" in data:
        data["bias_rating"], data["bias_score"] = parse_rating_field(
            data.pop("bias_rating_raw")
        )
    else:
        data["bias_rating"] = None
        data["bias_score"] = None

    # Parse factual reporting into label + score
    if "factual_reporting_raw" in data:
        data["factual_reporting"], data["factual_score"] = parse_rating_field(
            data.pop("factual_reporting_raw")
        )
    else:
        data["factual_reporting"] = None
        data["factual_score"] = None

    return data


def load_existing_results(filepath):
    """Load already-scraped results from JSON for resume support."""
    if not os.path.exists(filepath):
        return {}
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            records = json.load(f)
        return {r["url"]: r for r in records}
    except (json.JSONDecodeError, KeyError):
        return {}


def save_results(results, csv_path, json_path):
    """Save results to both CSV and JSON."""
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        for row in results:
            writer.writerow({k: row.get(k, "") for k in FIELDNAMES})


def main():
    print("MBFC Scraper")
    print("=" * 60)

    # Phase 1: Collect all source links from category pages
    print("\nPhase 1: Collecting source links from all categories...")
    all_sources = []  # list of (name, url, category)
    for category_name, path in CATEGORIES.items():
        sources = scrape_category(category_name, path)
        if MAX_PER_CATEGORY:
            sources = sources[:MAX_PER_CATEGORY]
        for name, link in sources:
            all_sources.append((name, link, category_name))
        time.sleep(3 + random.uniform(1, 2))

    print(f"\nTotal sources found: {len(all_sources)}")

    # Phase 2: Scrape individual MBFC source pages
    print("\nPhase 2: Scraping MBFC source pages for bias/credibility...")
    existing = load_existing_results(OUTPUT_JSON)
    results = list(existing.values())
    scraped_urls = set(existing.keys())

    skipped = 0
    errors = 0

    scraped_count = 0
    try:
        for i, (name, url, category) in enumerate(all_sources, 1):
            if url in scraped_urls:
                skipped += 1
                continue

            print(f"  [{i}/{len(all_sources)}] {name}")
            try:
                data = scrape_source(url)
                data["name"] = name
                data["url"] = url
                data["category"] = category

                # Filter to US sources only
                if data.get("country", "").upper() != "USA":
                    scraped_urls.add(url)
                    scraped_count += 1
                    continue

                results.append(data)
                scraped_urls.add(url)
            except Exception as e:
                print(f"    ERROR: {e}")
                errors += 1

            scraped_count += 1

            # Save progress every 25 sources scraped
            if scraped_count % 25 == 0:
                save_results(results, OUTPUT_CSV, OUTPUT_JSON)
                print(f"  [Progress saved: {len(results)} US sources]")

            delay()
    except KeyboardInterrupt:
        print("\n\n  Interrupted! Saving progress...")

    # Final save
    save_results(results, OUTPUT_CSV, OUTPUT_JSON)

    print("\n" + "=" * 60)
    print(f"Scraped {len(results)} sources.")
    print(f"  Skipped (already scraped): {skipped}")
    print(f"  Errors: {errors}")
    print(f"  Output: {OUTPUT_CSV}, {OUTPUT_JSON}")


if __name__ == "__main__":
    main()

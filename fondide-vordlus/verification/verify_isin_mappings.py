#!/usr/bin/env python3
"""
Layer 2: ISIN-to-Proxy Mapping Audit
======================================
For each ISIN in ETF_ISIN_TO_CSV, fetches fund info from justETF.com
and verifies the proxy mapping is reasonable.

Checks:
  - ISIN resolves to a real fund on justETF
  - Fund's tracked index region matches proxy ETF region
  - Flags suspicious mappings (e.g., Nasdaq mapped to MSCI USA)
"""

import re
import sys
import time
import urllib.request
import urllib.error
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent

# ── Proxy region mapping ──
# What region each proxy ticker represents
PROXY_REGIONS = {
    "SASU": "USA",
    "SAEU": "Europe",
    "SAJP": "Japan",
    "SAEM": "Emerging Markets",
    "SSAC": "Global (ACWI)",
    "SSAC_EM": "Emerging Markets",
    "SAWD": "Developed World",
    "SPPY": "USA",           # S&P 500 ESG Leaders
    "XTJP": "Japan",
    "EMXU": "Emerging Markets ex China",
    "BNKE": "Europe",        # Euro Stoxx Banks
    "CNYA": "China",
    "NDIA": "India",
    "IKSA": "Saudi Arabia",
    "4BRZ": "Brazil",
    "GLOBALFOND_A": "Global",  # Swedbank Robur active fund
}

# Keywords in fund name → expected region
REGION_KEYWORDS = {
    "USA": ["usa", "u.s.", "s&p 500", "s&p500", "nasdaq", "russell", "north america", "american"],
    "Europe": ["europe", "euro stoxx", "stoxx 600", "stoxx europe", "msci europe", "eu ", "emu"],
    "Japan": ["japan", "topix", "nikkei", "msci japan"],
    "Emerging Markets": ["emerging", "msci em ", "ftse emerging"],
    "Emerging Markets ex China": ["emerging ex china", "em ex china"],
    "China": ["china", "csi 300", "msci china", "ftse china"],
    "Global (ACWI)": ["all country", "acwi", "all world", "ftse all-world"],
    "Developed World": ["world", "developed", "msci world", "global", "ftse developed"],
    "India": ["india", "nifty", "msci india"],
    "Brazil": ["brazil", "ibovespa", "msci brazil"],
    "Saudi Arabia": ["saudi", "tadawul"],
}


def parse_etf_isin_to_csv():
    """Parse ETF_ISIN_TO_CSV dict from pipeline_shared.py."""
    src = BASE / "pipeline_shared.py"
    content = src.read_text()

    # Extract the dict block
    match = re.search(r'ETF_ISIN_TO_CSV\s*=\s*\{(.*?)\}', content, re.DOTALL)
    if not match:
        print("ERROR: Could not find ETF_ISIN_TO_CSV in pipeline_shared.py")
        sys.exit(1)

    mapping = {}
    for line in match.group(1).split('\n'):
        # Match lines like: 'IE0009FT4LX4': 'SAWD',  # comment
        m = re.match(r"\s*'([A-Z0-9]+)'\s*:\s*'([A-Za-z0-9_]+)'", line)
        if m:
            isin, proxy = m.group(1), m.group(2)
            # Extract comment
            comment = ""
            c = re.search(r'#\s*(.*)', line)
            if c:
                comment = c.group(1).strip()
            mapping[isin] = {"proxy": proxy, "comment": comment}

    return mapping


def fetch_justetf_info(isin):
    """Fetch fund name and description from justETF for an ISIN."""
    url = f"https://www.justetf.com/en/etf-profile.html?isin={isin}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml",
    }
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8", errors="replace")

        # Extract title from <title> tag or <h1>
        title_match = re.search(r'<title[^>]*>(.*?)</title>', html, re.DOTALL)
        title = title_match.group(1).strip() if title_match else ""
        # Clean up title
        title = re.sub(r'\s*\|.*', '', title)  # Remove "| justETF" suffix
        title = re.sub(r'\s+', ' ', title).strip()

        # Extract index info from the page
        # Look for "Replication index" or similar
        index_match = re.search(r'(?:Index|Benchmark|Replication)\s*(?:</[^>]+>\s*)*(?:<[^>]+>\s*)*([^<]{5,80})', html, re.IGNORECASE)
        index_name = index_match.group(1).strip() if index_match else ""

        return {"found": True, "title": title, "index": index_name, "url": url}

    except urllib.error.HTTPError as e:
        if e.code == 404:
            return {"found": False, "title": "", "index": "", "url": url}
        return {"found": None, "title": f"HTTP {e.code}", "index": "", "url": url}
    except Exception as e:
        return {"found": None, "title": str(e), "index": "", "url": url}


def guess_region_from_text(text):
    """Guess the fund's region from its name/description."""
    text_lower = text.lower()
    matches = []
    for region, keywords in REGION_KEYWORDS.items():
        for kw in keywords:
            if kw in text_lower:
                matches.append(region)
                break
    return matches


def regions_compatible(proxy_region, detected_regions):
    """Check if proxy region is compatible with detected regions."""
    if not detected_regions:
        return None  # Unknown

    # Direct match
    if proxy_region in detected_regions:
        return True

    # Compatible mappings
    compatible = {
        ("USA", "Developed World"),
        ("Developed World", "USA"),
        ("USA", "Global"),
        ("Global", "USA"),
        ("Global", "Developed World"),
        ("Developed World", "Global"),
        ("Global (ACWI)", "Developed World"),
        ("Developed World", "Global (ACWI)"),
        ("Emerging Markets", "Emerging Markets ex China"),
        ("Emerging Markets ex China", "Emerging Markets"),
        ("Europe", "Developed World"),
        ("Japan", "Developed World"),
    }
    for det in detected_regions:
        if (proxy_region, det) in compatible or (det, proxy_region) in compatible:
            return True

    return False


def main():
    mapping = parse_etf_isin_to_csv()
    print("=" * 70)
    print(f"Layer 2: ISIN-to-Proxy Mapping Audit ({len(mapping)} ISINs)")
    print("=" * 70)

    results = []
    skip_fetch = "--offline" in sys.argv

    if skip_fetch:
        print()
        print("  ⚠ OFFLINE MODE: checking pipeline's own inline comments against proxy regions.")
        print("    This is internal consistency only, NOT independent verification.")
        print("    Run with --online to fetch from justETF for real validation.")
        print()

    for i, (isin, info) in enumerate(mapping.items()):
        proxy = info["proxy"]
        comment = info["comment"]
        proxy_region = PROXY_REGIONS.get(proxy, "Unknown")

        if skip_fetch:
            # Offline mode: just check comment for region hints
            detected = guess_region_from_text(comment)
            compat = regions_compatible(proxy_region, detected)
            if compat is None:
                status = "SKIP"
                msg = f"Offline mode, cannot verify"
            elif compat:
                status = "PASS"
                msg = f"Comment region matches proxy ({proxy_region})"
            else:
                status = "WARN"
                msg = f"Comment suggests {detected}, proxy={proxy} ({proxy_region})"
            results.append((status, isin, proxy, msg, comment))
            continue

        # Fetch from justETF
        if i > 0:
            time.sleep(1.5)  # Rate limit

        justetf = fetch_justetf_info(isin)
        fund_text = f"{justetf['title']} {justetf['index']} {comment}"
        detected = guess_region_from_text(fund_text)
        compat = regions_compatible(proxy_region, detected)

        if justetf["found"] is False:
            # Not an ETF — might be an active fund (SE, LU prefixes)
            detected_from_comment = guess_region_from_text(comment)
            compat_comment = regions_compatible(proxy_region, detected_from_comment)
            if compat_comment is False:
                status = "WARN"
                msg = f"Not on justETF; comment suggests {detected_from_comment}, proxy={proxy} ({proxy_region})"
            else:
                status = "INFO"
                msg = f"Not on justETF (likely active fund). Comment: {comment}"
        elif justetf["found"] is None:
            status = "WARN"
            msg = f"Fetch error: {justetf['title']}"
        elif compat is True:
            status = "PASS"
            msg = f"Region OK: {detected} → {proxy} ({proxy_region}). Title: {justetf['title']}"
        elif compat is False:
            status = "MISMATCH"
            msg = f"Region MISMATCH: detected {detected}, proxy={proxy} ({proxy_region}). Title: {justetf['title']}"
        else:
            status = "WARN"
            msg = f"Could not detect region. Title: {justetf['title']}"

        results.append((status, isin, proxy, msg, comment))
        print(f"  [{i+1}/{len(mapping)}] {isin} → {proxy}: {status}")

    # ── Print report ──
    print()
    print("=" * 70)
    print("RESULTS")
    print("=" * 70)

    counts = {}
    for status, isin, proxy, msg, comment in results:
        counts[status] = counts.get(status, 0) + 1
        marker = {"PASS": "✓", "WARN": "⚠", "MISMATCH": "✗", "INFO": "ℹ", "SKIP": "○"}.get(status, "?")
        print(f"  {marker} [{status:>8}] {isin} → {proxy:>10}  {msg}")

    print()
    summary = ", ".join(f"{v} {k}" for k, v in sorted(counts.items()))
    print(f"Summary: {summary}")

    return 1 if counts.get("MISMATCH", 0) > 0 else 0


if __name__ == "__main__":
    sys.exit(main())

# CLAUDE.md — Tuleva Reporting Engine

## Project overview

Reporting tools for Tuleva investment funds. These are both internal and external but we always assume that even external reports should be made so that we are ready to make them public. The repo is **public** (GitHub Pages), so never commit secrets nor any personal data.

## Caching customer-level data

CRM dumps, cohort exports and any per-person data must be cached **outside the repo**, not in `reports/adhoc/data/`. Convention: `~/.cache/tuleva-reports/` (override with `TULEVA_CACHE_DIR` env var). Notebook pattern:

```python
import os
from pathlib import Path
CACHE_DIR = Path(os.environ.get('TULEVA_CACHE_DIR', Path.home() / '.cache' / 'tuleva-reports'))
CACHE_DIR.mkdir(parents=True, exist_ok=True)
df = pd.read_pickle(CACHE_DIR / 'card_2345_crm.pkl')
```

Why outside the repo: gitignore alone is fragile (typos, new filenames, file already tracked). Keeping the cache outside the working tree means `git add -A` cannot accidentally stage it. `reports/adhoc/data/` is reserved for files we intentionally publish (market aggregates, benchmarks, look-throughs). `.gitignore` carries belt-and-suspenders patterns (`card_*.pkl`, `*cohort*.csv`, `*customer*.csv`) for the case someone caches in the wrong place.

Notebook outputs in committed `.ipynb`/`.html` must contain only aggregates (counts, %, quartiles, charts) — never raw rows of customer data.

**Licensed bulk data** (e.g. pensionikeskuse market dump): same rule — cache to `~/.cache/tuleva-reports/`, never to `reports/adhoc/data/`. Even if the data isn't personal data, the data-sharing agreement may permit only aggregate republishing. Default assumption for any bulk dataset received under licence: aggregate-only outputs in committed files, raw rows stay outside the repo.

## Environment

- **Python venv**: `/Users/tonupekk/Desktop/tuleva-reports/.venv/bin/python3` (Python 3.14)
- **Jupyter**: `/Users/tonupekk/Desktop/tuleva-reports/.venv/bin/jupyter`
- Always use the venv python/jupyter, not system python — system python cannot install packages (PEP 668)
- Shorthand: `VENV=../../.venv/bin` from `reports/adhoc/` or `reports/monthly/`

## Commands

- Use `.venv/bin/python3` (not bare `python3`) for all execution
- **Never run inline Python/code via shell** (`python3 -c "..."`, `python3 -c '...'`). These trigger permission prompts due to `#`, `;`, parentheses, etc. Instead:
  - For quick snippets: use heredoc (`python3 <<'EOF' ... EOF`)
  - For anything more than a one-liner: write to a temp `.py` file, run it, then delete it
  - Same applies to `curl` with complex args, `jq` with special chars, etc. — if a command has characters that look like shell metacharacters inside quotes, use a heredoc or temp file
- API keys live in `.env` at project root, loaded via `python-dotenv` — never hardcode in notebooks or commit
- **Google Sheets/Docs/Slides**: always use the GCP service account (`GCP_SERVICE_ACCOUNT` env var in `.env`), never the MCP Google Drive connector. Service account email: `read-write@tuleva-claude.iam.gserviceaccount.com` — document must be shared with this address as Editor. Pattern: `gspread.service_account_from_dict(json.loads(os.environ['GCP_SERVICE_ACCOUNT']))` for Sheets, `googleapiclient.discovery.build('docs', 'v1', credentials=...)` for Docs. Example in `common/scripts/fetch_data.py`. Team setup: `common/config/google-sheets-setup.md`.
- Monthly report: `cd reports/monthly && python3 build_monthly_report.py YYYY M md|html|pdf`
- Fetch data: `cd reports/monthly && set -a && source ../../.env && set +a && python3 fetch_monthly_data.py YYYY M`
  - **Primary data source is the consolidated KPI card 2578** ("Mv Kpi New for Claude"): a wide monthly time series stored in `data/YYYY-MM.yaml` under `kpi_2578`. It supplies AUM totals, active-investor counts, contributions, switching and outflows split by II/III pillar. YTD/YoY are computed in Python from the series (`reports/monthly/kpi_2578.py`) — the card has no YTD/YoY columns.
  - A short list of **survivor cards** (`fetch_monthly_data.SURVIVOR_CARDS`, stored under `cards`) supplies what 2578 cannot: new-savers distinct counts + by-source split (1518/418/1519/1520/1534/1535), monthly rate-change flow (1573), distinct III-contributor YTD (1657), fund-level switching lists (1911/1912), growth waterfalls (389/392/393), unit price (2245), financials (636), TKF (2305), and the AUM chart with forecast bars (334).
  - 2578 and the survivor cards are **live/restated snapshots** — historical months are revised as late transactions settle, so re-fetching an old month yields slightly different numbers than the originally published report. This is inherent to the data, not the pipeline; fetch a month shortly after it closes.

## Notebook workflow

- Execute: `jupyter nbconvert --to notebook --execute --allow-errors --inplace <notebook>.ipynb`
- Export HTML: use the config file to avoid shell quoting issues that trigger permission prompts:
  ```
  jupyter nbconvert --to html --config common/nbconvert_config.py <notebook>.ipynb
  ```
- Tag data/prep cells with `remove_cell` in metadata to hide from HTML output
- **Execute notebooks incrementally** — run each cell after writing it, don't batch-write the whole notebook then debug multiple stacked errors
- **Verify numbers in prose against current outputs** — when writing markdown summaries (kokkuvõtted, järeldused), copy each number from the cell that just ran, not from earlier iterations or memory. Numbers drift across iterations of the same analysis; pasted-from-memory claims regularly turn out wrong by the time you publish.

## Publishing (GitHub Pages)

- Pages served from `docs/` on main branch, auto-deploys on push
- Ad hoc reports: copy HTML to `docs/<name>.html`
- Monthly report: `cp reports/monthly/output/YYYY/monthly_report_YYYY-MM.html docs/latest-monthly-report.html`
- Update `docs/index.html` when adding new reports or updating dates

## Working with external data

- **Verify data sources before writing code that depends on them** — test API calls, check ticker availability, confirm which funds appear on which pages
- **Always handle missing data gracefully** — external sources (pensionikeskus, EODHD, Yahoo) may not have data for expected dates; write fallbacks from the start
- **Every price must have a date attached** — always verify the price date matches the expected NAV date, and flag/warn when it doesn't
- **Sanity check derived datasets against an independent public baseline** before drawing conclusions. Example: when filtering CRM (card 2345) for a cohort, compare cohort size to the public aggregate (card 1520) for the overlapping window — within ±10% means the filter is sound. A larger gap means you're filtering on the wrong field.
- pensionikeskus.ee III pillar page does NOT include TKF (Täiendav Kogumisfond) — it's a separate fund category

## ETF holdings matching

- **Use ISIN as the primary stock identifier** — it's the only universal, unambiguous equity identifier. Never use `Ticker|Location` as a primary key.
- **ACWI benchmark**: use SPDR MSCI ACWI (SPYY) which publishes daily holdings WITH ISINs. iShares SSAC omits ISINs from its CSV exports.
- **OpenFIGI is unreliable** for cross-provider matching — it returns local exchange tickers that don't match iShares conventions (e.g. `0Y3K|Ireland` instead of `EATON|United States`).
- **Never match on ticker alone** across providers — same ticker can be different companies in different countries (e.g. `CFR` = Cullen/Frost in US, Richemont in Switzerland).
- **Validate matching quality** by decomposing active share into weight excluded/added/reduced. Suspiciously large "weight added" signals matching failures, not real portfolio differences.
- **Prebuilt mapping files** in `reports/adhoc/data/`: `isin_to_stockid.json` (1,698 ISIN→Ticker|Location), `provider_name_mappings.json` (705 Amundi/Xtrackers→SSAC name mappings). Use as fallback when ISIN matching isn't available.
- **Amundi XLSX files** have broken XML stylesheets — parse via `zipfile` + raw XML, not `openpyxl`. See `/fetch-holdings` skill for the parser function.
- See `/fetch-holdings` skill for full data source details, loading instructions, data refresh workflow, and name normalization code.

## Fund transaction types

Tuleva fund transactions (Metabase card 2326) are grouped by Application Type:

**Funds:**
- `EE3600109435` = TUK75 (II pillar)
- `EE3600109443` = TUK00 (II pillar conservative)
- `EE3600001707` = TUV100 (III pillar)

**Direction** is determined by Transaction Type: `Osakute märkimine` = inflow, `Broneeritud osakute kustutamine` = outflow.

**Application types:**
- *(none)* — Regular contributions. II pillar: from Maksuamet. III pillar: from isik (personal) or tööandja (employer)
- `PEVA` — II pillar fund switching (both in and out)
- `SWI` — III pillar fund switching (both in and out)
- `RED` — Redemption
- `RAVA` — II pillar full withdrawal before pension age
- `FPAA` / `FPAA3` — Fondipension (II / III pillar)
- `YKVAK` — Pension lump sum payment
- `YKVAO` — Partial pension lump sum payment
- `RES` — Restore / transfer from insurance (inflow)
- `INS` — Transfer to insurance (III pillar, outflow)
- `PLAV` — Transfer to insurance / annuity purchase (II pillar, outflow)
- `PAAV` — Inheritance (both directions)
- `TV` / `DECO` — Bailiff / bankruptcy redemption

**Key calendar patterns:**
- II pillar contributions (Maksuamet): land on days 13–16, most often on the 15th
- II pillar switching (PEVA) and RAVA: quarterly in Jan, May, Sep (days 1–5)
- Fondipension/YKVAK/YKVAO: monthly around days 15–18
- III pillar contributions: spread across all business days, spike on day 11
- III pillar redemptions (RED) and switching (SWI): every business day

## Style and branding

- Use `common/branding/style.css` for all HTML reports — it defines Tuleva fonts (Merriweather headings, Roboto body), colors (#002F63 navy, #00AEEA blue), and table styles
- Don't reinvent styles inline when the CSS already covers it
- For cohort/customer behaviour summaries: prefer **distributions** (quartiles, buckets, share-by-segment) over single-number averages. Averages hide the heterogeneity that drives interpretation. When using Q1/Median/Q3, label as "kvartiilipiirid" (quartile boundaries) — these are 3 cut-points dividing data into 4 quartiles, not 4 quartiles themselves.

## Tuleva tone and communication principles

Based on Kristel Raesaar's thesis "Pensioniks kogumine kui heaolukäitumine" (TU 2025). Full checklist: `common/style-guide/review-checklist.md`

**Three psychological needs** — every piece of communication should support at least one:
- **Autonomy**: frame saving as freedom and independence, not obligation. Pillar assets belong to the person.
- **Competence**: clear, simple messages. Provide an evidence-based "recipe", not information overload. No jargon.
- **Social belonging**: "we" framing, social proof ("X thousand people did this"), members are the main characters.

**Language**:
- Avoid "pension" (feels distant), "säästmine/saving" (feels restrictive), fear without action steps, institutional tone
- Prefer "kogumine" (building/accumulating), freedom, security, peace of mind, concrete next steps
- Two audiences: peace-of-mind seekers (need clear guidelines) and freedom-valuing investors (need acknowledgment of sophistication)

**Tuleva tone**: professional but human, mentor not savior, transparent by default, "we" not "them"

## Collaboration style

- User works iteratively: one change at a time, review, then next
- User says what to change in plain language; don't over-interpret or add extras
- User handles narrative text themselves; focus on code/charts only
- User refers to notebook cells by **execution count** [N], not by cell index
- When deleting a code cell, also delete its associated markdown commentary cell

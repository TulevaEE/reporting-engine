# CLAUDE.md — Tuleva Reporting Engine

## Project overview

Reporting tools for Tuleva investment funds. These are both internal and external but we always assume that even external reports should be made so that we are ready to make them public. The repo is **public** (GitHub Pages), so never commit secrets nor any personal data.

## Commands

- Use `python3` (not `python`) on this machine
- **Never run inline Python/code via shell** (`python3 -c "..."`, `python3 -c '...'`). These trigger permission prompts due to `#`, `;`, parentheses, etc. Instead:
  - For quick snippets: use heredoc (`python3 <<'EOF' ... EOF`)
  - For anything more than a one-liner: write to a temp `.py` file, run it, then delete it
  - Same applies to `curl` with complex args, `jq` with special chars, etc. — if a command has characters that look like shell metacharacters inside quotes, use a heredoc or temp file
- API keys live in `.env` at project root, loaded via `python-dotenv` — never hardcode in notebooks or commit
- Monthly report: `cd reports/monthly && python3 build_monthly_report.py YYYY M md|html|pdf`
- Fetch data: `cd reports/monthly && set -a && source ../../.env && set +a && python3 fetch_monthly_data.py YYYY M`

## Notebook workflow

- Execute: `jupyter nbconvert --to notebook --execute --allow-errors --inplace <notebook>.ipynb`
- Export HTML: use the config file to avoid shell quoting issues that trigger permission prompts:
  ```
  jupyter nbconvert --to html --config common/nbconvert_config.py <notebook>.ipynb
  ```
- Tag data/prep cells with `remove_cell` in metadata to hide from HTML output
- **Execute notebooks incrementally** — run each cell after writing it, don't batch-write the whole notebook then debug multiple stacked errors

## Publishing (GitHub Pages)

- Pages served from `docs/` on main branch, auto-deploys on push
- Ad hoc reports: copy HTML to `docs/<name>.html`
- Monthly report: `cp reports/monthly/output/YYYY/monthly_report_YYYY-MM.html docs/latest-monthly-report.html`
- Update `docs/index.html` when adding new reports or updating dates

## Working with external data

- **Verify data sources before writing code that depends on them** — test API calls, check ticker availability, confirm which funds appear on which pages
- **Always handle missing data gracefully** — external sources (pensionikeskus, EODHD, Yahoo) may not have data for expected dates; write fallbacks from the start
- **Every price must have a date attached** — always verify the price date matches the expected NAV date, and flag/warn when it doesn't
- pensionikeskus.ee III pillar page does NOT include TKF (Täiendav Kogumisfond) — it's a separate fund category

## ETF holdings matching

- **Use ISIN as the primary stock identifier** — it's the only universal, unambiguous equity identifier. Never use `Ticker|Location` as a primary key.
- **ACWI benchmark**: use SPDR MSCI ACWI (SPYY) which publishes daily holdings WITH ISINs. iShares SSAC omits ISINs from its CSV exports.
- **OpenFIGI is unreliable** for cross-provider matching — it returns local exchange tickers that don't match iShares conventions (e.g. `0Y3K|Ireland` instead of `EATON|United States`).
- **Never match on ticker alone** across providers — same ticker can be different companies in different countries (e.g. `CFR` = Cullen/Frost in US, Richemont in Switzerland).
- **Validate matching quality** by decomposing active share into weight excluded/added/reduced. Suspiciously large "weight added" signals matching failures, not real portfolio differences.
- See `/fetch-holdings` skill for full data source details, loading instructions, and name normalization code.

## Style and branding

- Use `common/branding/style.css` for all HTML reports — it defines Tuleva fonts (Merriweather headings, Roboto body), colors (#002F63 navy, #00AEEA blue), and table styles
- Don't reinvent styles inline when the CSS already covers it

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

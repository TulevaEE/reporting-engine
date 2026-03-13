# Monthly Data Update

Update after each month's fund reports are published (~15th of following month).

1. Copy `2026-01.json` → `YYYY-MM.json`, update `month`, `reports` URLs, and `allocations` weights
2. In `export_fund_data.py` line ~2410, change `MONTH = 'YYYY-MM'`
   — this single variable propagates to subtitle, CSV disclaimer, and footer freshness notice
3. Run `python export_fund_data.py`
4. Verify: open `docs/fondide-vordlus/index.html`, confirm footer shows new month

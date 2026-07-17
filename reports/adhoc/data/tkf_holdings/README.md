# TKF100 holdings — data sources

Holdings files for the TKF100 active-share analysis ([`../../active_share_tkf.ipynb`](../../active_share_tkf.ipynb)).
These are **manual downloads** (not committed — they change daily and are large). Re-download
all of them **on the same day** so the holdings dates line up with the live SPDR ACWI benchmark.

For every file, download the **Excel "Holdings / Constituents / Composition" export — NOT a PDF factsheet.**
Save each with the exact filename below, in this directory. The notebook reads the dates from each
file's modification time.

| Save as | ISIN | Fund | Download URL |
|---|---|---|---|
| `xtrackers_usa.xlsx`    | IE00BJZ2DC62 | Xtrackers MSCI USA Screened UCITS ETF 1C            | https://etf.dws.com/en-gb/IE00BJZ2DC62-msci-usa-screened-ucits-etf-1c/ |
| `xtrackers_canada.xlsx` | LU0476289540 | Xtrackers MSCI Canada Screened UCITS ETF 1C         | https://etf.dws.com/en-gb/LU0476289540-msci-canada-screened-ucits-etf-1c/ |
| `amundi_usa.xlsx`       | IE000F60HVH9 | Amundi MSCI USA Screened UCITS ETF Acc              | https://www.amundietf.lu/en/professional/products/equity/amundi-msci-usa-screened-ucits-etf-acc/ie000f60hvh9 |
| `vanguard_na.xlsx`      | IE000O58J820 | Vanguard ESG North America All Cap UCITS ETF (USD) Acc | https://www.vanguardinvestor.co.uk/investments/vanguard-esg-north-america-all-cap-ucits-etf-usd-accumulating |
| `bnp_europe.xlsx`       | LU1291099718 | BNP Paribas Easy MSCI Europe Min TE UCITS ETF       | https://www.bnpparibas-am.com/en-be/professional-investor/fundsheet/equity/bnp-paribas-easy-msci-europe-esg-filtered-min-te-ucits-etf-c-lu1291099718/ |
| `bnp_pacific.xlsx`      | LU1291106356 | BNP Paribas Easy MSCI Pacific ex Japan Min TE UCITS ETF | https://www.bnpparibas-am.com/en-lu/professional-investor/fundsheet/equity/bnp-paribas-easy-msci-pacific-ex-japan-esg-filtered-min-te-ucits-etf-c-lu1291106356/ |
| `bnp_japan.xlsx`        | LU1291102447 | BNP Paribas Easy MSCI Japan Min TE UCITS ETF        | https://www.bnpparibas-am.com/en-lu/professional-investor/fundsheet/equity/bnp-paribas-easy-msci-japan-esg-filtered-min-te-ucits-etf-c-lu1291102447/ |
| `invesco_em.csv` *(or .xlsx)* | IE00BMDBMY19 | Invesco MSCI EM Universal Screened UCITS ETF Acc | https://www.invesco.com/uk/en/financial-products/etfs/invesco-msci-emerging-markets-universal-screened-ucits-etf-acc.html → Portfolio Holdings |

## Notes

- **Auto-fetched live (no download needed):**
  - SPDR MSCI ACWI (SPYY) benchmark — `ssga.com` daily holdings xlsx.
  - iShares Developed World Screened (SAWD) — fetched from the iShares website in the notebook.
- **Invesco EM** — preferred source is the **official full-holdings** file above. The notebook can
  fall back to scraping `companiesmarketcap.com`, but that is the latest snapshot only and covers
  ~93% of fund weight (the small-cap tail is dropped), which inflates EM active share.
- **BNP Paribas** — use the **fundsheet pages** above and export the *Composition* in Excel. Do **not**
  use `docfinder.bnpparibas-am.com/...` links — those return PDF factsheets, which the loaders can't parse.
- **DWS / Vanguard** pages render via JavaScript, so the Excel download button only appears in a browser
  (a scripted `curl` sees an empty shell — that's expected).

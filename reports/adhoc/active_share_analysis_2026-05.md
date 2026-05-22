# Active Share Analysis — Proposed Equity Model Portfolio vs MSCI ACWI

**22 May 2026**

## Portfolio

| # | Holding | ISIN | Weight | OCF (bps) |
|---|---------|------|--------|-----------|
| 1 | Amundi MSCI USA Screened UCITS ETF Acc | IE000F60HVH9 | 28.0% | 5 |
| 2 | Xtrackers MSCI USA Screened UCITS ETF 1C | IE00BJZ2DC62 | 28.0% | 7 |
| 3 | iShares MSCI USA Screened UCITS ETF | IE00BFNM3G45 | 7.6% | 7 |
| 4 | Amundi MSCI World Ex USA Screened UCITS ETF Acc | FR0013209921 | 24.8% | 20 |
| 5 | iShares MSCI EM IMI Screened UCITS ETF | IE00BFNM3P36 | 11.6% | 18 |
| | **Total** | | **100.0%** | **~9.6** |

## Active Share

| Metric | Value |
|--------|-------|
| **Active Share** | **12.48%** |
| Benchmark | SPDR MSCI ACWI (SPYY GY) |
| Benchmark date | 21 May 2026 |
| Holdings dates | 20–22 May 2026 |
| Matching method | ISIN-based |
| Overlapping stocks | 1,964 |
| Portfolio-only stocks | 24 |
| ACWI-only stocks (ESG-excluded) | 295 |

## Active Share Decomposition

| Component | Active Share % |
|-----------|---------------|
| ESG exclusions (363 stocks absent from portfolio) | 4.2% |
| Weight differences in shared stocks | 8.2% |
| Other / interaction | 0.1% |
| **Total active share** | **12.48%** |

## Comparison with Current TUK75/TUV100

| | Current TUK75 | Proposed | Change |
|---|--------------|----------|--------|
| Active share | 11.57% | 12.48% | +0.91pp |
| Weighted avg OCF | 7.4 bps | ~9.6 bps | +2.2 bps |
| EM weight | 9.9% | 11.6% | +1.7pp (= ACWI) |
| ETF providers | 1 (iShares) | 3 (Amundi, Xtrackers, iShares) | |
| DM ex-US coverage | Partial (no CA, AU, HK, IL, NZ, SG) | Full (via Amundi World Ex USA) | |

## Largest Absent Positions (ESG-excluded)

| # | Stock | ACWI % | ISIN |
|---|-------|--------|------|
| 1 | Chevron Corporation | 0.343% | US1667641005 |
| 2 | Procter & Gamble Company | 0.316% | US7427181091 |
| 3 | Coca-Cola Company | 0.309% | US1912161007 |
| 4 | Philip Morris International Inc. | 0.302% | US7181721090 |
| 5 | Nestlé S.A. | 0.256% | CH0038863350 |
| 6 | BHP Group Ltd | 0.237% | AU000000BHP4 |
| 7 | PepsiCo Inc. | 0.204% | US7134481081 |
| 8 | Boeing Company | 0.182% | US0970231058 |
| 9 | Siemens Energy AG | 0.161% | DE000ENER6Y0 |
| 10 | ConocoPhillips | 0.154% | US20825C1045 |

## Top Overweight Positions

| # | Stock | Portfolio % | ACWI % | Diff |
|---|-------|------------|--------|------|
| 1 | NVIDIA Corp | 5.748% | 5.231% | +0.517% |
| 2 | Apple Inc | 4.702% | 4.456% | +0.246% |
| 3 | Microsoft Corp | 3.147% | 2.923% | +0.223% |
| 4 | Alphabet Inc Class A | 2.396% | 2.185% | +0.211% |
| 5 | Alphabet Inc Class C | 1.983% | 1.832% | +0.151% |
| 6 | Broadcom Inc | 1.992% | 1.855% | +0.138% |
| 7 | Amazon.com Inc | 2.699% | 2.576% | +0.123% |
| 8 | Walmart Inc | 0.606% | 0.520% | +0.086% |
| 9 | Tesla Inc | 1.250% | 1.166% | +0.083% |
| 10 | Meta Platforms Inc | 1.396% | 1.320% | +0.076% |

## Data Sources

- **Benchmark:** SPDR MSCI ACWI UCITS ETF (SPYY GY), holdings as of 21 May 2026. Downloaded from ssga.com. 2,259 equities, ISIN-based.
- **Amundi USA Screened (IE000F60HVH9):** Fund Holdings XLSX from Amundi, 20 May 2026. 477 equities with ISINs.
- **Xtrackers USA Screened (IE00BJZ2DC62):** Constituent XLSX from DWS, 22 May 2026. 484 equities with ISINs.
- **iShares USA Screened (IE00BFNM3G45):** iShares CSV (SASU), 20 May 2026. 485 equities, bridged to ISINs via SPDR name matching (479/485 matched).
- **Amundi World Ex USA Screened (FR0013209921):** Fund Holdings XLSX from Amundi, 20 May 2026. 690 equities with ISINs.
- **iShares EM IMI Screened (IE00BFNM3P36):** iShares CSV (SAEM), 20 May 2026. 2,648 equities, bridged to ISINs via SPDR name matching (1,108/2,648 matched — remainder are IMI small-caps not in ACWI).

## Methodology

Active Share = ½ × Σ|w_fund − w_benchmark| (Cremers & Petajisto 2009)

All matching performed on ISIN. For iShares funds (which publish holdings without ISINs), a name-based bridge was built against the SPDR ACWI holdings using normalized company names with prefix+country fallback.

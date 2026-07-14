"""
Helpers for reading the consolidated KPI card 2578 ("Mv Kpi New for Claude").

Card 2578 is a monthly time series — one row per month, keyed by a ``Month``
label like ``"Jun-26"`` (English three-letter month + two-digit year, the same
format card 334 uses). It carries current-month values only: there are no
YTD/YoY/growth columns, so those are computed here from the full series.

Shared by both build_monthly_report.py and generate_monthly_charts.py.
"""

MONTHS_ABBR = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
               'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
_ABBR_TO_NUM = {abbr: i + 1 for i, abbr in enumerate(MONTHS_ABBR)}


def month_label(year: int, month: int) -> str:
    """(2026, 6) -> 'Jun-26'."""
    return f"{MONTHS_ABBR[month - 1]}-{year % 100:02d}"


def parse_label(label: str) -> tuple:
    """'Jun-26' -> (2026, 6). Assumes 20xx (the fund launched in 2017)."""
    abbr, yy = label.split('-')
    return 2000 + int(yy), _ABBR_TO_NUM[abbr]


def index_series(series: list) -> dict:
    """Build a ``(year, month) -> row`` index from the 2578 series (O(1) lookup)."""
    idx = {}
    for row in series or []:
        label = row.get('Month')
        if not label:
            continue
        try:
            idx[parse_label(label)] = row
        except (KeyError, ValueError):
            continue
    return idx


def get_row(series: list, year: int, month: int) -> dict:
    """Return the row for (year, month), or None."""
    return index_series(series).get((year, month))


def get_prev_year_row(series: list, year: int, month: int) -> dict:
    """Return the same-month row one year earlier, or None."""
    return index_series(series).get((year - 1, month))


def _value(row: dict, column: str) -> float:
    """Column value, treating a missing row/None as 0."""
    if not row:
        return 0
    v = row.get(column)
    return v if v is not None else 0


def ytd_sum(series: list, year: int, month: int, column: str) -> float:
    """Sum ``column`` over Jan..month of ``year``."""
    idx = index_series(series)
    return sum(_value(idx.get((year, m)), column) for m in range(1, month + 1))


def ytd_prev_sum(series: list, year: int, month: int, column: str) -> float:
    """Sum ``column`` over Jan..month of the previous year (for YTD YoY)."""
    idx = index_series(series)
    return sum(_value(idx.get((year - 1, m)), column) for m in range(1, month + 1))


def yoy(series: list, year: int, month: int, column: str):
    """(cur - prev) / prev on the same-month rows, or None if not computable."""
    idx = index_series(series)
    cur, prev = idx.get((year, month)), idx.get((year - 1, month))
    if cur is None or prev is None:
        return None
    c, p = cur.get(column), prev.get(column)
    if not p:
        return None
    return (c - p) / p

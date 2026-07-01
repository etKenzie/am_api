"""SQL helper for optional start_date/end_date loan filters."""

import calendar


def append_date_filters(
    query: str,
    params: dict,
    *,
    start_date: str = None,
    end_date: str = None,
    date_column: str = "l.proses_date",
) -> str:
    if start_date and end_date:
        query += f" AND {date_column} >= :start_date"
        query += f" AND {date_column} <= :end_date"
        params["start_date"] = start_date
        params["end_date"] = end_date
    return query


def month_bounds(month: int, year: int) -> tuple[str, str]:
    start_date = f"{year}-{month:02d}-01"
    last_day = calendar.monthrange(year, month)[1]
    end_date = f"{year}-{month:02d}-{last_day:02d}"
    return start_date, end_date

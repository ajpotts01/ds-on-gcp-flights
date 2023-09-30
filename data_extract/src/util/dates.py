import datetime as dt
from typing import Tuple


def get_next_month(year: str, month: str) -> Tuple[str, str]:
    # Always get middle of the month
    full_date: dt.datetime = dt.datetime(year=int(year), month=int(month), day=15)
    full_date_next_month: dt.datetime = full_date + dt.timedelta(days=30)

    return str(full_date_next_month.year), str(full_date_next_month.month)

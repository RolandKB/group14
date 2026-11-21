"""Find the cheapest complete operating window using historical Gold prices.

The algorithm accepts hourly price rows.

All comparisons use UTC timestamps.
"""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal

HOUR = timedelta(hours=1)


@dataclass(frozen=True)
class Schedule:
    start_time: datetime
    end_time: datetime
    duration_hours: int
    power_kw: Decimal
    energy_kwh: Decimal
    estimated_cost_nok: Decimal


def _utc(value):
    """Require timestamps with timezone info and convert them to UTC."""
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise ValueError("Timestamps must be timezone datetime objects")
    return value.astimezone(UTC)


def find_cheapest_window(
    prices, earliest_start, latest_end, duration_hours, power_kw=1
):
    """Find the cheapest uninterrupted operating window."""

    first = _utc(earliest_start)
    last = _utc(latest_end)
    if (
        not isinstance(duration_hours, int)
        or isinstance(duration_hours, bool)
        or duration_hours < 1
    ):
        raise ValueError("duration_hours must be a positive integer")
    
    power = Decimal(str(power_kw))
    if not power.is_finite() or power <= 0:
        raise ValueError("power_kw must be a positive finite number")
    
    if first >= last:
        raise ValueError("earliest_start must precede latest_end")
    
    if (
        first.minute
        or first.second
        or first.microsecond
        or last.minute
        or last.second
        or last.microsecond
    ):
        raise ValueError("Window boundaries must be aligned to whole hours")

    # Keep only unique valid hourly slots.
    slots = {}
    duplicates = set()
    
    for row in prices:
        start = row["start_time"]
        end = row["end_time"]
        price = row["nok_per_kwh"]
        
        if start is None or end is None or price is None:
            continue
        
        if start.tzinfo is None:
            start = start.replace(tzinfo=UTC)  # PySpark UTC timestamp
        
        if end.tzinfo is None:
            end = end.replace(tzinfo=UTC)
        
        start, end = _utc(start), _utc(end)
        if end - start != HOUR or start < first or end > last:
            continue
        
        amount = Decimal(str(price))
        if not amount.is_finite():
            continue
        
        if start in slots:
            duplicates.add(start)
        else:
            slots[start] = amount
    
    for start in duplicates:
        slots.pop(start, None)

    best = None
    current = first
    
    while current + duration_hours * HOUR <= last:
        hours = [current + i * HOUR for i in range(duration_hours)]
        
        if all(hour in slots for hour in hours):
            cost = sum((slots[hour] * power for hour in hours), Decimal(0))
            
            # Strictly cheaper only.
            if best is None or cost < best.estimated_cost_nok:
                best = Schedule(
                    start_time=current,
                    end_time=current + duration_hours * HOUR,
                    duration_hours=duration_hours,
                    power_kw=power,
                    energy_kwh=power * duration_hours,
                    estimated_cost_nok=cost,
                )

        current += HOUR
    return best


def recommend(prices_df, area, local_start, local_end, duration_hours, power_kw):
    """Run a historical scheduling request against the Gold Spark Dataframe."""
    from zoneinfo import ZoneInfo

    from pyspark.sql import functions as F

    oslo = ZoneInfo("Europe/Oslo")
    first = datetime.fromisoformat(local_start).replace(tzinfo=oslo).astimezone(UTC)
    last = datetime.fromisoformat(local_end).replace(tzinfo=oslo).astimezone(UTC)

    selected = (
        prices_df.filter(F.col("price_area") == area)
        .filter(F.col("start_time").cast("long") >= int(first.timestamp()))
        .filter(F.col("end_time").cast("long") <= int(last.timestamp()))
        .select("start_time", "end_time", "nok_per_kwh")
        .orderBy("start_time")
        .collect()
    )

    result = find_cheapest_window(selected, first, last, duration_hours, power_kw)

    print(f"Area: {area} | Allowed: {local_start} to {local_end}")
    print(f"Device: {power_kw} kW for {duration_hours} consecutive hours")
    print("Available price slots:", len(selected))
    
    if result is None:
        print("No complete operating window: missing prices or insufficient time.")
    else:
        print(
            "Recommended:",
            result.start_time.astimezone(oslo),
            "to",
            result.end_time.astimezone(oslo),
        )
        print("Energy:", result.energy_kwh, "kWh")
        print("Estimated spot-energy cost:", f"{result.estimated_cost_nok:.2f}", "NOK")
    return result

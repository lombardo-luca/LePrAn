"""Shared analytics helpers used by GUI and snapshot import paths."""


def compute_budget_range_buckets(film_budget_data: dict) -> dict:
    """Compute budget range buckets from per-film budget data."""
    bucket_defs = [
        {"range": "$200M+", "min": 200_000_000, "max": None},
        {"range": "$100–$200M", "min": 100_000_000, "max": 200_000_000},
        {"range": "$50–$100M", "min": 50_000_000, "max": 100_000_000},
        {"range": "$20–$50M", "min": 20_000_000, "max": 50_000_000},
        {"range": "$5–$20M", "min": 5_000_000, "max": 20_000_000},
        {"range": "$1–$5M", "min": 1_000_000, "max": 5_000_000},
        {"range": "$0–$1M", "min": 0, "max": 1_000_000},
    ]
    unknown_count = 0
    total_with_budget = 0

    for budget in film_budget_data.values():
        if budget is None or budget <= 0:
            unknown_count += 1
            continue
        total_with_budget += 1
        for bucket in bucket_defs:
            if bucket["max"] is None:
                if budget >= bucket["min"]:
                    bucket["count"] = bucket.get("count", 0) + 1
                    break
            elif bucket["min"] <= budget < bucket["max"]:
                bucket["count"] = bucket.get("count", 0) + 1
                break
        else:
            unknown_count += 1

    result_buckets = []
    for bucket in bucket_defs:
        count = bucket.get("count", 0)
        if count > 0:
            percent = (count / total_with_budget * 100) if total_with_budget > 0 else 0.0
            result_buckets.append({
                "range": bucket["range"],
                "count": count,
                "percent": f"{percent:.1f}"
            })

    if unknown_count > 0:
        total = total_with_budget + unknown_count
        percent = (unknown_count / total * 100) if total > 0 else 0.0
        result_buckets.append({
            "range": "Unknown / Not reported",
            "count": unknown_count,
            "percent": f"{percent:.1f}"
        })

    return {
        "buckets": result_buckets,
        "totalFilmsWithBudget": total_with_budget
    }

"""FX-translation demonstration — proves the exchange_rates seed + the mart
formula reproduce the reference `usd_amount`, with no database and no
subsidiary sources.

The non-USD marts compute `usd_amount = round(fx_amount * rate_to_usd, 2)` by
joining `stg__exchange_rates` on year+month, where the rate is the daily
`exchange_rates` seed averaged per calendar month (the original Fabric
`avg_yearmonth_rates` design). The subsidiary sources that feed those marts
aren't shipped in this repo (they model a redacted live environment), so we
can't run the pipeline end-to-end here. Instead we apply the exact same
monthly rate table and formula to the committed reference dataset
(`demo/sales_data.parquet`, which carries both `fx_amount` and the
already-translated `usd_amount`) and assert the rebuilt USD matches — which is
what proves the FX layer is correct.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parents[1]
SEED = ROOT / "dbt" / "seeds" / "exchange_rates.csv"
SALES = ROOT / "demo" / "sales_data.parquet"
SUBS = ROOT / "demo" / "subsidiaries.parquet"

# Subsidiary -> functional currency. HLM and HLS both report in INR; HL/HLB
# are USD-functional and skip translation (rate = 1).
SUB_CURRENCY = {
    "HL": "USD", "HLB": "USD", "HLC": "CZK",
    "HLD": "EUR", "HLM": "INR", "HLP": "PLN", "HLS": "INR",
}
# Loose real-world sanity bounds (USD per 1 unit of the currency).
RATE_BANDS = {
    "EUR": (0.90, 1.30), "CZK": (0.03, 0.06),
    "INR": (0.008, 0.02), "PLN": (0.18, 0.32),
}

pytestmark = pytest.mark.skipif(
    not (SEED.exists() and SALES.exists() and SUBS.exists()),
    reason="seed or reference parquet not present",
)


@pytest.fixture(scope="module")
def rebuilt() -> pd.DataFrame:
    """Reference sales with `usd_amount` rebuilt from the seed via the mart formula.

    Mirrors the marts: average the daily seed rate per (currency, yearmonth),
    join sales on year+month, then usd = round(fx_amount * rate_to_usd, 2).
    """
    rates = pd.read_csv(SEED, dtype={"currency": str})
    rates["date"] = pd.to_datetime(rates["date"])
    rates["yearmonth"] = rates["date"].dt.year * 100 + rates["date"].dt.month
    monthly = rates.groupby(["currency", "yearmonth"], as_index=False)["rate_to_usd"].mean()

    sales = pd.read_parquet(SALES)
    sales["currency"] = sales["subsidiary"].map(SUB_CURRENCY)
    sales["date"] = pd.to_datetime(sales["date"])
    sales["yearmonth"] = sales["date"].dt.year * 100 + sales["date"].dt.month
    for col in ("fx_amount", "usd_amount"):
        sales[col] = pd.to_numeric(sales[col], errors="coerce")

    m = sales.merge(monthly, on=["currency", "yearmonth"], how="left")
    # USD subs translate at 1.0; everything else uses the joined monthly rate.
    m.loc[m["currency"] == "USD", "rate_to_usd"] = 1.0
    m["usd_rebuilt"] = (m["fx_amount"] * m["rate_to_usd"]).round(2)
    return m


def test_seed_covers_every_sales_month(rebuilt):
    """Every non-USD sales row finds a monthly rate — no NULLs that would break
    the not_null contract on usd_amount."""
    missing = rebuilt[(rebuilt["currency"] != "USD") & rebuilt["rate_to_usd"].isna()]
    assert missing.empty, f"{len(missing)} sales rows have no rate"


@pytest.mark.parametrize("currency,bounds", RATE_BANDS.items())
def test_rates_are_realistic(currency, bounds):
    rates = pd.read_csv(SEED)
    lo, hi = bounds
    band = rates.loc[rates["currency"] == currency, "rate_to_usd"]
    assert band.between(lo, hi).all(), (
        f"{currency} rates outside {bounds}: "
        f"min={band.min()} max={band.max()}"
    )


def test_usd_functional_subs_are_unchanged(rebuilt):
    usd = rebuilt[rebuilt["subsidiary"].isin(["HL", "HLB"])]
    assert (usd["usd_rebuilt"] == usd["usd_amount"]).all()


def test_translation_direction(rebuilt):
    """EUR (> USD) translates up; INR (< USD) translates down."""
    eur = rebuilt[rebuilt["currency"] == "EUR"]
    inr = rebuilt[rebuilt["currency"] == "INR"]
    assert (eur["usd_rebuilt"].sum() > eur["fx_amount"].sum())
    assert (inr["usd_rebuilt"].sum() < inr["fx_amount"].sum())


def test_per_subsidiary_usd_reconciles(rebuilt):
    """Rebuilt USD reproduces the reference per subsidiary within rounding."""
    g = rebuilt.groupby("subsidiary").agg(
        ref=("usd_amount", "sum"), reb=("usd_rebuilt", "sum")
    )
    g["rel"] = (g["reb"] - g["ref"]).abs() / g["ref"]
    worst = g["rel"].max()
    assert worst < 1e-4, f"worst per-sub USD drift {worst:.2e}:\n{g}"


def test_grand_total_usd_reconciles(rebuilt):
    ref = rebuilt["usd_amount"].sum()
    reb = rebuilt["usd_rebuilt"].sum()
    assert abs(reb - ref) / ref < 1e-6, f"grand total drift: ref={ref} reb={reb}"

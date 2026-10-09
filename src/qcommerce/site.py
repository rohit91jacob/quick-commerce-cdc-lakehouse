"""``qc report``: a static results site (no JavaScript, no external assets) built from the lakehouse.

Real and simulated numbers are labelled as such on the page. Colour carries no meaning: price moves
are shown with signed numbers and arrows.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from typing import Any

from qcommerce import trino_client
from qcommerce.settings import Settings

REPO_URL = "https://github.com/rohit91jacob/quick-commerce-cdc-lakehouse"

QUERIES: dict[str, str] = {
    "meta": """
        SELECT count(*), max(source_created_at),
               count_if(source_created_at > current_timestamp - interval '1' day),
               count(DISTINCT currency), count(DISTINCT product_code)
        FROM silver.market_prices WHERE NOT _is_deleted""",
    "by_currency": """
        SELECT currency, count(*) AS observations, count(DISTINCT product_code) AS products,
               count(DISTINCT location_id) AS shops
        FROM silver.market_prices
        WHERE NOT _is_deleted AND source_created_at > current_timestamp - interval '7' day
        GROUP BY 1 ORDER BY 2 DESC LIMIT 12""",
    "movers": """
        SELECT product_name, brand, shop_name, city, country_code, currency, previous_price, price, change_pct,
               observed_on
        FROM gold.mart_price_changes
        WHERE change_pct <> 0 AND observed_on >= current_date - interval '30' day
        ORDER BY abs(change_pct) DESC, observed_on DESC LIMIT 15""",
    "index": """
        SELECT category_tag, currency, observed_on, series_observed, price_index
        FROM (SELECT *, row_number() OVER (PARTITION BY category_tag, currency ORDER BY observed_on DESC) AS rn
              FROM gold.mart_category_price_index_daily
              WHERE category_tag <> 'en:unknown')
        WHERE rn = 1 AND series_observed >= 3
        ORDER BY series_observed DESC LIMIT 15""",
    "india": """
        SELECT coalesce(mp.name, p.product_code), mp.brand, l.city, p.price, p.observed_on
        FROM silver.market_prices p
        LEFT JOIN silver.market_products mp ON mp.market_product_id = p.market_product_id
        LEFT JOIN silver.market_locations l ON l.location_id = p.location_id
        WHERE NOT p._is_deleted AND p.currency = 'INR'
        ORDER BY p.source_created_at DESC LIMIT 12""",
    "coverage": """
        SELECT category_name, products, real_products, real_priced_products
        FROM gold.mart_catalogue_coverage ORDER BY real_products DESC, category_name""",
    "ops": """
        SELECT sum(delivered_orders),
               sum(pct_within_promise * delivered_orders) / nullif(sum(delivered_orders), 0),
               max(p90_delivery_minutes)
        FROM gold.mart_delivery_sla_hourly
        WHERE order_date >= current_date - interval '7' day""",
    "basket": """
        SELECT sum(gross_merchandise_value) / nullif(sum(delivered_orders), 0),
               sum(real_price_value_share * gross_merchandise_value) / nullif(sum(gross_merchandise_value), 0),
               sum(real_product_value_share * gross_merchandise_value) / nullif(sum(gross_merchandise_value), 0)
        FROM gold.mart_basket_daily
        WHERE order_date >= current_date - interval '7' day""",
}

CSS = """
:root { color-scheme: light dark; }
body { margin: 0; font: 15px/1.5 system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;
  background: #fcfcfb; color: #0b0b0b; }
@media (prefers-color-scheme: dark) { body { background: #1a1a19; color: #ffffff; }
  .muted, th { color: #c3c2b7 !important; } tbody tr:nth-child(even) { background: #262624 !important; }
  .scroll, th { border-color: #383835 !important; } .tag { border-color: #c3c2b7 !important; } }
main { max-width: 1080px; margin: 0 auto; padding: 24px 16px 48px; }
h1 { font-size: 1.6rem; margin: 0 0 4px; } h2 { font-size: 1.15rem; margin: 34px 0 4px; }
.muted { color: #52514e; } p.note { margin: 0 0 10px; font-size: .9rem; }
.scroll { overflow-x: auto; border: 1px solid #dedcd5; border-radius: 8px; }
table { border-collapse: collapse; width: 100%; font-variant-numeric: tabular-nums; }
th, td { padding: 6px 10px; text-align: right; white-space: nowrap; }
th { color: #52514e; font-weight: 600; border-bottom: 1px solid #dedcd5; }
th.l, td.l { text-align: left; } td.wrap { white-space: normal; text-align: left; }
tbody tr:nth-child(even) { background: #f0efec; }
.tag { display: inline-block; font-size: .72rem; font-weight: 700; letter-spacing: .04em; padding: 1px 6px;
  border: 1px solid #52514e; border-radius: 4px; vertical-align: middle; margin-left: 6px; }
.kpis { display: flex; flex-wrap: wrap; gap: 24px; margin: 10px 0; }
.kpi b { display: block; font-size: 1.4rem; }
footer { margin-top: 40px; font-size: .85rem; } a { color: inherit; }
"""


def collect(settings: Settings) -> dict[str, Any]:
    conn = trino_client.connect(settings.trino)
    data: dict[str, Any] = {"generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
    for name, sql in QUERIES.items():
        data[name] = [list(row) for row in trino_client.query(conn, sql)]
    return data


def _fmt(value: Any, digits: int = 2) -> str:
    if value is None:
        return "–"
    if isinstance(value, float):
        return f"{value:,.{digits}f}"
    return escape(str(value))


def _pct(value: Any) -> str:
    if value is None:
        return "–"
    v = float(value) * 100
    arrow = "▲" if v > 0 else ("▼" if v < 0 else "")
    return f"{arrow} {v:+.1f}%"


def _table(headers: list[tuple[str, str]], rows: list[list[str]]) -> str:
    head = "".join(f'<th class="{c}" scope="col">{escape(h)}</th>' for h, c in headers)
    body = "".join(
        "<tr>"
        + "".join(f'<td class="{c}">{cell}</td>' for (_, c), cell in zip(headers, r, strict=True))
        + "</tr>"
        for r in rows
    )
    if not rows:
        body = f'<tr><td class="l" colspan="{len(headers)}">No data yet.</td></tr>'
    return f'<div class="scroll"><table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>'


def render(data: dict[str, Any]) -> str:
    total, latest, last_day, currencies, products = (data["meta"] or [[0, None, 0, 0, 0]])[0]
    delivered, within, p90 = (data["ops"] or [[None, None, None]])[0]
    aov, real_price_share, real_product_share = (data["basket"] or [[None, None, None]])[0]
    latest_txt = escape(str(latest)[:16]) if latest else "–"

    currency_rows = [[escape(str(c)), _fmt(n), _fmt(p), _fmt(s)] for c, n, p, s in data["by_currency"]]
    mover_rows = [
        [
            escape(str(name or "–")),
            escape(str(brand or "–")),
            escape(f"{shop or '–'}, {city or ''} {country or ''}".strip(" ,")),
            escape(str(cur)),
            _fmt(prev),
            _fmt(price),
            _pct(pct),
            escape(str(day)),
        ]
        for name, brand, shop, city, country, cur, prev, price, pct, day in data["movers"]
    ]
    index_rows = [
        [escape(str(tag).removeprefix("en:").replace("-", " ")), escape(str(cur)), escape(str(day)), _fmt(n),
         _fmt(float(idx), 1)]
        for tag, cur, day, n, idx in data["index"]
    ]  # fmt: skip
    india_rows = [
        [
            escape(str(name)),
            escape(str(brand or "–")),
            escape(str(city or "–")),
            f"₹{_fmt(price)}",
            escape(str(day)),
        ]
        for name, brand, city, price, day in data["india"]
    ]
    coverage_rows = [[escape(str(c)), _fmt(n), _fmt(r), _fmt(rp)] for c, n, r, rp in data["coverage"]]

    def share(v: Any) -> str:
        return "–" if v is None else f"{float(v) * 100:.0f}%"

    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Quick-commerce CDC lakehouse — live results</title>
<meta name="description" content="Real shelf prices (Open Prices) and a simulated 10-minute grocery store, rebuilt every 4 hours.">
<style>{CSS}</style></head><body><main>
<h1>Quick-commerce CDC lakehouse</h1>
<p class="muted">Rebuilt every 4 hours by <a href="{REPO_URL}">quick-commerce-cdc-lakehouse</a> ·
latest real price observation {latest_txt} UTC · generated {escape(data["generated_at"][:16].replace("T", " "))} UTC</p>

<h2>Real shelf prices<span class="tag">REAL</span></h2>
<p class="note">Crowdsourced price observations from <a href="https://prices.openfoodfacts.org">Open Prices</a>, loaded
incrementally into Postgres and carried to the lakehouse by CDC.</p>
<div class="kpis"><div class="kpi"><b>{_fmt(total)}</b>observations loaded</div>
<div class="kpi"><b>{_fmt(last_day)}</b>in the last 24 h</div>
<div class="kpi"><b>{_fmt(products)}</b>products</div><div class="kpi"><b>{_fmt(currencies)}</b>currencies</div></div>
<h2>Last 7 days by currency<span class="tag">REAL</span></h2>
{_table([("Currency", "l"), ("Observations", ""), ("Products", ""), ("Shops", "")], currency_rows)}

<h2>Biggest price moves (30 days)<span class="tag">REAL</span></h2>
<p class="note">Change between consecutive observations of the same product at the same shop.</p>
{_table([("Product", "l"), ("Brand", "l"), ("Shop", "l"), ("Cur.", "l"), ("Before", ""), ("After", ""), ("Change", ""), ("Observed", "l")], mover_rows)}

<h2>Category price index<span class="tag">REAL</span></h2>
<p class="note">Geometric mean of each product-shop series' price relative to its first observation (= 100); latest day
per category and currency, categories with at least 3 series.</p>
{_table([("Category", "l"), ("Currency", "l"), ("As of", "l"), ("Series", ""), ("Index", "")], index_rows)}

<h2>Latest real prices in India<span class="tag">REAL</span></h2>
{_table([("Product", "l"), ("Brand", "l"), ("City", "l"), ("Price", ""), ("Observed", "l")], india_rows)}

<h2>Store catalogue provenance</h2>
<p class="note">The simulated store sells real Open Food Facts products (barcodes issued by GS1 India) where the
category is covered; their selling price follows the latest real INR price once Open Prices has one.</p>
{_table([("Category", "l"), ("Products", ""), ("Real products", ""), ("Real INR price", "")], coverage_rows)}

<h2>Store operations, last 7 days<span class="tag">SIMULATED</span></h2>
<p class="note">Customers, orders, riders and deliveries are simulated: real quick-commerce orders are not public.</p>
<div class="kpis"><div class="kpi"><b>{_fmt(delivered)}</b>orders delivered</div>
<div class="kpi"><b>{share(within)}</b>within the 10-minute promise</div>
<div class="kpi"><b>{_fmt(p90, 1)} min</b>worst hourly P90</div>
<div class="kpi"><b>₹{_fmt(aov)}</b>average basket</div>
<div class="kpi"><b>{share(real_product_share)}</b>of basket value on real products</div>
<div class="kpi"><b>{share(real_price_share)}</b>at real prices</div></div>

<footer class="muted">Data: <a href="https://world.openfoodfacts.org">Open Food Facts</a> and
<a href="https://prices.openfoodfacts.org">Open Prices</a>, © their contributors, available under the
<a href="https://opendatacommons.org/licenses/odbl/1-0/">Open Database License</a>. Optional mandi prices:
data.gov.in (Government Open Data License – India). Store activity is simulated.
Numbers: <a href="data.json">data.json</a> · Code: <a href="{REPO_URL}">GitHub</a>.</footer>
</main></body></html>
"""


def build(settings: Settings, out: Path) -> Path:
    data = collect(settings)
    out.mkdir(parents=True, exist_ok=True)
    (out / "index.html").write_text(render(data), encoding="utf-8")
    (out / "data.json").write_text(json.dumps(data, default=str, indent=1), encoding="utf-8")
    (out / ".nojekyll").write_text("", encoding="utf-8")
    return out / "index.html"

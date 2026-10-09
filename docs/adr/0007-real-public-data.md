# ADR 0007: Real public data next to the simulation

**Status:** accepted

## Context

The original OLTP was fully synthetic. Real quick-commerce orders (Blinkit, Zepto, Instacart) are
not public, but two parts of the business have real, openly licensed sources:

- **What is sold:** Open Food Facts has real products, including tens of thousands tagged as sold in
  India.
- **What things cost:** Open Prices has crowdsourced shelf prices from real shops, a few hundred new
  observations a day.

## Decision

- **Catalogue.** A committed snapshot (`src/qcommerce/realdata/off_india_catalogue.json.gz`) holds up
  to 60 real products per covered category. Products already priced at an Indian shop on Open Prices
  come first, then products with GS1 India barcodes (prefix 890). Seeding uses them as `OFF-<barcode>`
  SKUs before falling back to simulated items. Categories Open Food Facts doesn't cover stay simulated.
- **Market prices are their own subject area.** `commerce.market_locations`, `market_products` and
  `market_prices` mirror Open Prices with their real ids and currencies. They are not converted into
  rupees or attached to the fictional stores, because that would fabricate data.
- **Store prices follow real INR prices.** After each sync, a catalogue product whose barcode has an
  INR observation takes the latest one as its selling price (`price_source = 'open_prices'`), and MRP
  is raised if needed. Simulated orders are therefore priced from real shelf prices where they exist.
- **Two incremental streams with cursors** in `ops.ingest_cursors`:
  - a global stream (`created__gte`, ascending), looking back 14 days on the first run;
  - an INR-only stream over the whole history, because Indian observations are rare (about 400 in
    total) and they are what prices the store.

  Each page and its cursor commit in one transaction, and the boundary is re-read inclusively.
  Upserts only touch rows whose values changed, so re-runs produce no CDC events.
- **Agmarknet is optional.** It needs a free data.gov.in key (`QC_DATA_GOV_IN_API_KEY`) and is skipped
  and logged without one.
- **CI stays deterministic.** The e2e job runs `qc realdata sync --fixtures bundled` on two committed
  pages of real observations. The scheduled refresh uses the live API.

## Consequences

- CDC now carries real `INSERT`s (new observations) and real `UPDATE`s (repriced products), alongside
  the simulated workload. Migration V003 doubles as a second online schema change: new columns on
  `products`, plus new tables added to the publication.
- Real coverage depends on contributors. The global marts are multi-currency, and the India-specific
  numbers are small but real. The site labels every section REAL or SIMULATED.
- Licence: ODbL attribution appears in the README, on the site and in the fixtures directory.
  Committing extracts (the snapshot and the fixtures) carries the ODbL share-alike terms for those files.

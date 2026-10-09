"""The results page renders from query rows alone (no Trino needed) and labels real vs simulated data."""

from __future__ import annotations

from datetime import date, datetime

from qcommerce.site import render


def test_render_labels_sources_and_formats_moves() -> None:
    data = {
        "generated_at": "2026-10-09T08:00:00+00:00",
        "meta": [[8122, datetime(2026, 10, 9, 7, 15), 735, 31, 6900]],
        "by_currency": [["EUR", 2971, 1800, 120], ["INR", 12, 11, 7]],
        "movers": [
            [
                "Maggi Masala",
                "Maggi",
                "Reliance Smart",
                "Pune",
                "IN",
                "INR",
                14.0,
                15.0,
                0.0714,
                date(2026, 10, 8),
            ]
        ],
        "index": [["en:snacks", "EUR", date(2026, 10, 8), 40, 103.2]],
        "india": [["Parle-G", "Parle", "Delhi", 10.0, date(2026, 10, 7)]],
        "coverage": [["Snacks & Munchies", 32, 32, 30]],
        "ops": [[1500, 0.81, 14.2]],
        "basket": [[412.5, 0.48, 0.55]],
    }
    html = render(data)
    assert "<script" not in html
    assert html.count('class="tag">REAL') >= 4
    assert "SIMULATED" in html
    assert "▲ +7.1%" in html
    assert "Parle-G" in html and "₹10.00" in html
    assert "Open Database License" in html


def test_render_handles_an_empty_lake() -> None:
    empty = {k: [] for k in ("meta", "by_currency", "movers", "index", "india", "coverage", "ops", "basket")}
    html = render({"generated_at": "2026-10-09T08:00:00+00:00", **empty})
    assert "No data yet." in html

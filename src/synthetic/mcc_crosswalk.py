"""A confidence-scored OSM-tag -> MCC crosswalk, used ONLY to assign a plausible
MCC to synthetic transactions against real Silver merchants.

docs/11-data-model.md (written before any Silver code existed) already
anticipated exactly this need: "OpenStreetMap has no concept of MCC. Mapping
shop=/amenity= tags to MCC codes is a controlled, versioned, confidence-scored
reference table (ref_osm_category_to_mcc(osm_key, osm_value, mcc_code,
confidence, assumption_notes)), never an automated algorithm presented as
reliable." This module IS that table's first draft — but scoped strictly to
synthetic-OLTP MCC assignment, not to silver_merchant.

IMPORTANT: this does NOT touch silver_merchant.mcc_code, which remains NULL —
that field is explicitly reserved (docs/21 §3.5) for a future *approved* real
crosswalk, which this is not. This module produces a separate, synthetic-only
per-merchant MCC assignment consumed only by src/synthetic/generators.py.

Every mapping is independently verified present in real silver_mcc output
before being hardcoded here (see the S9 completion report for the verification
transcript). Confidence is deliberately low (<=0.9) everywhere — this is a
keyword heuristic over ~30 of the ~35 distinct real shop/amenity tag values
observed in the real Bronze OSM extract, not a certified classification.
"""

from typing import Any

# (osm_key, osm_value, mcc_code, confidence, assumption_notes)
REF_OSM_CATEGORY_TO_MCC: list[tuple[str, str, str, float, str]] = [
    ("amenity", "restaurant", "5812", 0.9, "direct match — Eating places and Restaurants"),
    ("amenity", "fast_food", "5814", 0.9, "direct match — Fast Food Restaurants"),
    ("amenity", "cafe", "5812", 0.6, "no distinct cafe MCC in this reference set; grouped with general eating places"),
    ("amenity", "bank", "6011", 0.7, "6011 is cash-disbursement; nearest available proxy for a bank branch"),
    ("shop", "clothes", "5651", 0.8, "Family Clothing Stores"),
    ("shop", "jewelry", "5944", 0.9, "Watch, Clock, Jewelry, and Silverware Stores"),
    ("shop", "hairdresser", "7230", 0.9, "Barber and Beauty Shops"),
    ("shop", "beauty", "7230", 0.7, "grouped with Barber and Beauty Shops"),
    ("shop", "shoes", "5661", 0.9, "Shoe Stores"),
    ("shop", "mobile_phone", "5732", 0.6, "no telecom-specific MCC in this set; grouped with Electronic Sales"),
    ("shop", "cosmetics", "5977", 0.9, "Cosmetic Stores"),
    ("shop", "gift", "5947", 0.8, "Card Shops, Gift, Novelty, and Souvenir Shops"),
    ("shop", "confectionery", "5441", 0.9, "Candy, Nut, and Confectionery Stores"),
    ("shop", "convenience", "5499", 0.8, "Misc. Food Stores — Convenience Stores and Specialty Markets"),
    ("shop", "optician", "8043", 0.9, "Opticians, Opticians Goods and Eyeglasses"),
    ("shop", "supermarket", "5411", 0.9, "Grocery Stores, Supermarkets"),
    ("shop", "tea", "5499", 0.4, "loose match to specialty food retail"),
    ("shop", "e-cigarette", "5993", 0.4, "loose proxy via tobacco-adjacent retail (Cigar Stores and Stands)"),
    ("shop", "seafood", "5422", 0.5, "loose match to Meat Provisioners — Freezer and Locker"),
    ("shop", "greengrocer", "5499", 0.5, "loose match to specialty food retail"),
    ("shop", "electronics", "5732", 0.8, "Electronic Sales"),
    ("shop", "chemist", "5912", 0.8, "Drug Stores and Pharmacies"),
    ("shop", "tailor", "5949", 0.6, "loose match to Sewing, Needle, Fabric, and Piece Goods Stores"),
    ("shop", "deli", "5499", 0.5, "loose match to specialty food retail"),
    ("shop", "butcher", "5422", 0.7, "Meat Provisioners — Freezer and Locker"),
    ("shop", "sports", "5941", 0.8, "Sporting Goods Stores"),
    ("shop", "florist", "5992", 0.9, "Florists"),
    ("shop", "books", "5942", 0.9, "direct match — Book Stores"),
]

FALLBACK_MCC = "5999"  # Miscellaneous and Specialty Retail Stores — verified present in real Silver data
FALLBACK_CONFIDENCE = 0.3
FALLBACK_NOTE = "no shop/amenity keyword match — low-confidence fallback to Miscellaneous Retail"

_LOOKUP: dict[tuple[str, str], tuple[str, float, str]] = {
    (osm_key, osm_value): (mcc_code, confidence, note) for osm_key, osm_value, mcc_code, confidence, note in REF_OSM_CATEGORY_TO_MCC
}


def assign_mcc(merchant: dict[str, Any]) -> tuple[str, float, str]:
    """Returns (mcc_code, confidence, assumption_note) for one real
    silver_merchant row. Checks shop before amenity (shop is the more specific
    OSM tag when both happen to be present)."""
    shop = merchant.get("raw_shop_tag")
    if shop:
        # Real data can be multi-valued ("art;gift" — see docs/21 §3.5); try
        # each sub-value in order, first match wins.
        for value in shop.split(";"):
            hit = _LOOKUP.get(("shop", value.strip()))
            if hit:
                return hit

    amenity = merchant.get("raw_amenity_tag")
    if amenity:
        hit = _LOOKUP.get(("amenity", amenity.strip()))
        if hit:
            return hit

    return FALLBACK_MCC, FALLBACK_CONFIDENCE, FALLBACK_NOTE

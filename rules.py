"""Rules engine for auto-labeling and auto-categorizing transactions.

Bank descriptions look like:
    DBT CRD 0819 04/07/26 84 Kindle Unltd*K31 440 Terry Ave N
    ACH TRAN TARGET DEBIT CRD WEB 902003054430965
    MOBILE PMT CAPITAL ONE WEB CA0D73EDC3F356A
    PAYMENT Atlantic Farms-P WEB 9UDAFATAWIWA

The "core" is the merchant name buried in the middle. We strip prefix noise
(transaction types, card numbers, dates) and suffix noise (addresses, reference
codes) to extract it, then use substring matching so that a pattern like
"EXPRESSVPN.COM" matches all ExpressVPN charges regardless of surrounding junk.
"""

import re

from db import get_db


def normalize_description(desc: str) -> str:
    """Strip bank noise from a description to extract the merchant core."""
    s = desc.strip()

    # Strip "DBT CRD 0819 04/07/26 84 " or "ATM W/D 1559 02/16/26 77 "
    s = re.sub(r'^(DBT CRD|ATM W/D)\s+\d{4}\s+\d{2}/\d{2}/\d{2}\s+\d{2}\s+', '', s)

    # Strip "POS DEB 1351 03/14/26 44 "
    s = re.sub(r'^POS DEB\s+\d{4}\s+\d{2}/\d{2}/\d{2}\s+\d{2}\s+', '', s)

    # Strip "ACH PMT " / "ACH TRAN "
    s = re.sub(r'^ACH\s+(PMT|TRAN)\s+', '', s)

    # Strip "MOBILE PMT "
    s = re.sub(r'^MOBILE PMT\s+', '', s)

    # Strip "PAYMENT "
    s = re.sub(r'^PAYMENT\s+', '', s)

    # Strip "Pre-auth Memo Hold / Pre auth "
    s = re.sub(r'^Pre-auth Memo Hold\s*/\s*Pre auth\s+', '', s)

    # Strip trailing "LOC: <location>"
    s = re.sub(r'\s+LOC:\s+\S+$', '', s)

    # Strip trailing "WEB <reference>"
    s = re.sub(r'\s+WEB\s+\S+$', '', s)

    # Strip trailing "TEL" (telephone payments)
    s = re.sub(r'\s+TEL$', '', s)

    # Strip trailing long alphanumeric reference codes (8+ chars, must contain at least one digit)
    s = re.sub(r'\s+(?=[A-Z0-9]*\d)[A-Z0-9]{8,}$', '', s)

    # Strip trailing street addresses: "<number> <words>" at the end
    s = re.sub(r'\s+\d+\s+[A-Za-z][\w\s]{5,}$', '', s)

    # Strip trailing "unknown"
    s = re.sub(r'\s+unknown$', '', s, flags=re.IGNORECASE)

    return s.strip()


def extract_pattern(desc: str) -> str:
    """Extract a fuzzy matching pattern from a bank description.

    Goes further than normalize_description by also stripping store/franchise
    numbers, sub-identifiers, and trailing location words — leaving just the
    merchant name core.
    """
    s = normalize_description(desc)

    # Strip store/franchise numbers: #8223, #8354, #R034
    s = re.sub(r'\s*#\w+', '', s)

    # Strip *sub-identifiers but keep text before *: "Kindle Unltd*K31" → "Kindle Unltd"
    s = re.sub(r'\*\S*', '', s)

    # Strip trailing run-together CITYSTATE or long location words (8+ chars, no dots/slashes)
    # Handles WESTBROOKME, PORTLANDME, WILMINGTON, etc.
    # Won't eat short merchant words like APPLE (5) or domains like EXPRESSVPN.COM
    s = re.sub(r'\s+[A-Z]{8,}$', '', s)

    return s.strip()


def _fuzzy_match(pattern: str, normalized: str) -> bool:
    """Check if pattern and normalized description refer to the same merchant.

    Uses bidirectional substring matching: either can be a substring of the other.
    This handles cases where the same merchant produces descriptions of varying
    lengths (e.g., "APPLE.COM/BILL" vs "APPLE.COM/BILL ONE APPLE PARK").
    """
    p = pattern.lower()
    n = normalized.lower()
    return p in n or n in p


def find_matching_transactions(db, pattern: str, exclude_id: str | None = None):
    """Find all transactions whose normalized description matches the pattern."""
    # We do the matching in Python since SQLite can't run our normalizer.
    # With hundreds/low thousands of transactions this is fine.
    all_txns = db.execute("SELECT id, description FROM transactions").fetchall()
    matches = []
    for txn in all_txns:
        if exclude_id and txn["id"] == exclude_id:
            continue
        norm = normalize_description(txn["description"])
        if _fuzzy_match(pattern, norm):
            matches.append(txn["id"])
    return matches


def apply_rules_to_transaction(db, txn_id: str, description: str):
    """Apply all matching rules to a single transaction. Returns True if any rule matched."""
    rules = db.execute("SELECT * FROM rules ORDER BY id").fetchall()
    norm = normalize_description(description).lower()
    applied = False

    for rule in rules:
        if _fuzzy_match(rule["pattern"], norm):
            updates = []
            params = []
            if rule["label"]:
                updates.append("label = ?")
                params.append(rule["label"])
            if rule["category_id"]:
                updates.append("category_id = ?")
                params.append(rule["category_id"])
            if updates:
                params.append(txn_id)
                db.execute(
                    f"UPDATE transactions SET {', '.join(updates)} WHERE id = ? AND label IS NULL AND category_id IS NULL",
                    params,
                )
                applied = True
            break  # First matching rule wins

    return applied


def apply_all_rules(db):
    """Apply rules to all unlabeled/uncategorized transactions."""
    txns = db.execute(
        "SELECT id, description FROM transactions WHERE label IS NULL OR category_id IS NULL"
    ).fetchall()
    count = 0
    for txn in txns:
        if apply_rules_to_transaction(db, txn["id"], txn["description"]):
            count += 1
    db.commit()
    return count

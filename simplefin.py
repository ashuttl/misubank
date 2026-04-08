"""SimpleFIN API client — fetches accounts and transactions."""

import os
import time
from urllib.parse import urlparse

import requests

from db import get_db


def _parse_access_url(access_url: str):
    """Extract base URL and credentials from the access URL."""
    parsed = urlparse(access_url)
    username = parsed.username
    password = parsed.password
    base_url = f"{parsed.scheme}://{parsed.hostname}{parsed.path}"
    return base_url, username, password


def fetch_accounts(access_url: str, start_date: int | None = None, end_date: int | None = None):
    """Fetch accounts and transactions from SimpleFIN."""
    base_url, username, password = _parse_access_url(access_url)

    params = {}
    if start_date is not None:
        params["start-date"] = start_date
    if end_date is not None:
        params["end-date"] = end_date
    params["pending"] = 1

    resp = requests.get(
        f"{base_url}/accounts",
        auth=(username, password),
        params=params,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def sync_transactions(access_url: str, days_back: int = 30):
    """Fetch recent transactions and upsert into the local database."""
    now = int(time.time())
    start = now - (days_back * 86400)

    data = fetch_accounts(access_url, start_date=start)

    db = get_db()
    accounts_synced = 0
    txns_synced = 0

    for acct in data.get("accounts", []):
        db.execute("""
            INSERT INTO accounts (id, name, currency, balance, available_balance, balance_date, conn_id, conn_name, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(id) DO UPDATE SET
                name=excluded.name, currency=excluded.currency, balance=excluded.balance,
                available_balance=excluded.available_balance, balance_date=excluded.balance_date,
                conn_id=excluded.conn_id, conn_name=excluded.conn_name, updated_at=CURRENT_TIMESTAMP
        """, (
            acct["id"], acct["name"], acct.get("currency", "USD"),
            acct.get("balance"), acct.get("available-balance"),
            acct.get("balance-date"),
            acct.get("conn_id"), None,
        ))
        accounts_synced += 1

        # Look up connection name
        conn_name = None
        for conn in data.get("connections", []):
            if conn["conn_id"] == acct.get("conn_id"):
                conn_name = conn.get("name")
                break
        if conn_name:
            db.execute("UPDATE accounts SET conn_name=? WHERE id=?", (conn_name, acct["id"]))

        for txn in acct.get("transactions", []):
            # Only insert new transactions — don't overwrite user edits (notes, tags, etc.)
            db.execute("""
                INSERT INTO transactions (id, account_id, posted, amount, description, pending, transacted_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    posted=excluded.posted, amount=excluded.amount,
                    description=excluded.description, pending=excluded.pending,
                    transacted_at=excluded.transacted_at
            """, (
                txn["id"], acct["id"], txn["posted"], txn["amount"],
                txn["description"], 1 if txn.get("pending") else 0,
                txn.get("transacted_at"),
            ))
            txns_synced += 1

    db.commit()
    db.close()

    return {"accounts": accounts_synced, "transactions": txns_synced, "errors": data.get("errors", [])}

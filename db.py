"""SQLite database for storing transactions, tags, notes, and cancelled subscriptions."""

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent / "misubank.db"


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _migrate(conn):
    """Add columns that may not exist yet on older databases."""
    # Check existing columns on transactions
    cols = {row[1] for row in conn.execute("PRAGMA table_info(transactions)").fetchall()}
    if "label" not in cols:
        conn.execute("ALTER TABLE transactions ADD COLUMN label TEXT")
    if "category_id" not in cols:
        conn.execute("ALTER TABLE transactions ADD COLUMN category_id INTEGER REFERENCES categories(id)")


def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS accounts (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            currency TEXT NOT NULL DEFAULT 'USD',
            balance TEXT,
            available_balance TEXT,
            balance_date INTEGER,
            conn_id TEXT,
            conn_name TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS transactions (
            id TEXT PRIMARY KEY,
            account_id TEXT NOT NULL REFERENCES accounts(id),
            posted INTEGER NOT NULL,
            amount TEXT NOT NULL,
            description TEXT NOT NULL,
            pending INTEGER DEFAULT 0,
            transacted_at INTEGER,
            note TEXT,
            is_subscription INTEGER DEFAULT 0,
            cancelled INTEGER DEFAULT 0,
            cancelled_at TIMESTAMP,
            monthly_amount TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            color TEXT DEFAULT '#6b7280'
        );

        CREATE TABLE IF NOT EXISTS rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pattern TEXT NOT NULL,
            label TEXT,
            category_id INTEGER REFERENCES categories(id),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS tags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            color TEXT DEFAULT '#6b7280'
        );

        CREATE TABLE IF NOT EXISTS transaction_tags (
            transaction_id TEXT NOT NULL REFERENCES transactions(id),
            tag_id INTEGER NOT NULL REFERENCES tags(id),
            PRIMARY KEY (transaction_id, tag_id)
        );

        CREATE INDEX IF NOT EXISTS idx_transactions_account ON transactions(account_id);
        CREATE INDEX IF NOT EXISTS idx_transactions_posted ON transactions(posted DESC);
        CREATE INDEX IF NOT EXISTS idx_transactions_subscription ON transactions(is_subscription);
        CREATE INDEX IF NOT EXISTS idx_transactions_cancelled ON transactions(cancelled);
    """)
    _migrate(conn)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_transactions_category ON transactions(category_id)")
    conn.commit()
    conn.close()

"""Misubank — local web app for reviewing transactions and tracking savings."""

import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from flask import Flask, g, jsonify, redirect, render_template, request, url_for

from db import get_db, init_db
from rules import apply_all_rules, extract_pattern, find_matching_transactions
from simplefin import sync_transactions

load_dotenv()

app = Flask(__name__)
app.secret_key = os.urandom(24)

SIMPLEFIN_ACCESS_URL = os.environ.get("SIMPLEFIN_ACCESS_URL", "").strip()


@app.before_request
def before_request():
    g.db = get_db()


@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db is not None:
        db.close()


# --- Pages ---


@app.route("/")
def index():
    """Main transaction list page."""
    tag_filter = request.args.get("tag")
    account_filter = request.args.get("account")
    search = request.args.get("q", "").strip()
    show = request.args.get("show", "all")  # all, subscriptions, cancelled

    category_filter = request.args.get("category")

    query = """
        SELECT t.*, a.name as account_name,
               c.name as category_name, c.color as category_color,
               GROUP_CONCAT(tg.name, ', ') as tag_names
        FROM transactions t
        JOIN accounts a ON t.account_id = a.id
        LEFT JOIN categories c ON t.category_id = c.id
        LEFT JOIN transaction_tags tt ON t.id = tt.transaction_id
        LEFT JOIN tags tg ON tt.tag_id = tg.id
    """
    conditions = []
    params = []

    if tag_filter:
        conditions.append("tg.name = ?")
        params.append(tag_filter)
    if account_filter:
        conditions.append("t.account_id = ?")
        params.append(account_filter)
    if category_filter:
        conditions.append("c.name = ?")
        params.append(category_filter)
    if search:
        conditions.append("(t.description LIKE ? OR t.label LIKE ?)")
        params.extend([f"%{search}%", f"%{search}%"])
    if show == "subscriptions":
        conditions.append("t.is_subscription = 1")
    elif show == "cancelled":
        conditions.append("t.cancelled = 1")
    elif show == "unlabeled":
        conditions.append("t.label IS NULL")

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += " GROUP BY t.id ORDER BY t.posted DESC"

    transactions = g.db.execute(query, params).fetchall()

    accounts = g.db.execute("SELECT id, name FROM accounts ORDER BY name").fetchall()
    tags = g.db.execute("SELECT id, name, color FROM tags ORDER BY name").fetchall()
    categories = g.db.execute("SELECT id, name, color FROM categories ORDER BY name").fetchall()

    return render_template(
        "index.html",
        transactions=transactions,
        accounts=accounts,
        tags=tags,
        categories=categories,
        filters={"tag": tag_filter, "account": account_filter, "category": category_filter, "q": search, "show": show},
    )


@app.route("/savings")
def savings():
    """Savings dashboard — shows cancelled subscriptions and monthly total."""
    cancelled = g.db.execute("""
        SELECT t.*, a.name as account_name
        FROM transactions t
        JOIN accounts a ON t.account_id = a.id
        WHERE t.cancelled = 1
        ORDER BY t.cancelled_at DESC
    """).fetchall()

    monthly_total = sum(abs(float(t["monthly_amount"] or t["amount"])) for t in cancelled)

    return render_template("savings.html", cancelled=cancelled, monthly_total=monthly_total)


@app.route("/tags")
def tags_page():
    """Tag management page."""
    tags = g.db.execute("""
        SELECT t.*, COUNT(tt.transaction_id) as txn_count
        FROM tags t
        LEFT JOIN transaction_tags tt ON t.id = tt.tag_id
        GROUP BY t.id
        ORDER BY t.name
    """).fetchall()
    return render_template("tags.html", tags=tags)


# --- API / htmx endpoints ---


@app.route("/sync", methods=["POST"])
def sync():
    """Trigger a sync from SimpleFIN."""
    if not SIMPLEFIN_ACCESS_URL:
        return jsonify({"error": "SIMPLEFIN_ACCESS_URL not set"}), 500
    days = int(request.form.get("days", 30))
    result = sync_transactions(SIMPLEFIN_ACCESS_URL, days_back=days)
    # Redirect back to the transactions page after sync
    return redirect(url_for("index"))


@app.route("/transaction/<path:txn_id>/note", methods=["POST"])
def update_note(txn_id):
    """Update a transaction's note."""
    note = request.form.get("note", "").strip()
    g.db.execute("UPDATE transactions SET note = ? WHERE id = ?", (note, txn_id))
    g.db.commit()
    if request.headers.get("HX-Request"):
        return render_template("partials/note_form.html", txn={"id": txn_id, "note": note})
    return redirect(url_for("index"))


@app.route("/transaction/<path:txn_id>/subscription", methods=["POST"])
def toggle_subscription(txn_id):
    """Toggle subscription flag on a transaction."""
    txn = g.db.execute("SELECT is_subscription FROM transactions WHERE id = ?", (txn_id,)).fetchone()
    new_val = 0 if txn["is_subscription"] else 1
    g.db.execute("UPDATE transactions SET is_subscription = ? WHERE id = ?", (new_val, txn_id))
    g.db.commit()
    if request.headers.get("HX-Request"):
        return render_template("partials/subscription_btn.html", txn={"id": txn_id, "is_subscription": new_val})
    return redirect(url_for("index"))


@app.route("/transaction/<path:txn_id>/cancel", methods=["POST"])
def mark_cancelled(txn_id):
    """Mark a subscription as cancelled and record the monthly savings."""
    monthly_amount = request.form.get("monthly_amount", "").strip()
    txn = g.db.execute("SELECT * FROM transactions WHERE id = ?", (txn_id,)).fetchone()

    if not monthly_amount:
        monthly_amount = txn["amount"]

    g.db.execute("""
        UPDATE transactions
        SET cancelled = 1, cancelled_at = CURRENT_TIMESTAMP,
            monthly_amount = ?, is_subscription = 1
        WHERE id = ?
    """, (monthly_amount, txn_id))
    g.db.commit()

    if request.headers.get("HX-Request"):
        updated = g.db.execute("SELECT * FROM transactions WHERE id = ?", (txn_id,)).fetchone()
        return render_template("partials/cancel_btn.html", txn=updated)
    return redirect(url_for("index"))


@app.route("/transaction/<path:txn_id>/uncancel", methods=["POST"])
def uncancel(txn_id):
    """Undo cancellation."""
    g.db.execute("""
        UPDATE transactions SET cancelled = 0, cancelled_at = NULL, monthly_amount = NULL WHERE id = ?
    """, (txn_id,))
    g.db.commit()
    if request.headers.get("HX-Request"):
        updated = g.db.execute("SELECT * FROM transactions WHERE id = ?", (txn_id,)).fetchone()
        return render_template("partials/cancel_btn.html", txn=updated)
    return redirect(url_for("index"))


@app.route("/transaction/<path:txn_id>/tag", methods=["POST"])
def add_tag(txn_id):
    """Add a tag to a transaction."""
    tag_name = request.form.get("tag_name", "").strip()
    if not tag_name:
        return redirect(url_for("index"))

    # Create tag if it doesn't exist
    g.db.execute("INSERT OR IGNORE INTO tags (name) VALUES (?)", (tag_name,))
    tag = g.db.execute("SELECT id FROM tags WHERE name = ?", (tag_name,)).fetchone()

    g.db.execute("INSERT OR IGNORE INTO transaction_tags (transaction_id, tag_id) VALUES (?, ?)",
                 (txn_id, tag["id"]))
    g.db.commit()

    if request.headers.get("HX-Request"):
        tags = g.db.execute("""
            SELECT tg.* FROM tags tg
            JOIN transaction_tags tt ON tg.id = tt.tag_id
            WHERE tt.transaction_id = ?
        """, (txn_id,)).fetchall()
        all_tags = g.db.execute("SELECT name FROM tags ORDER BY name").fetchall()
        return render_template("partials/tags.html", txn_id=txn_id, tags=tags, all_tags=all_tags)
    return redirect(url_for("index"))


@app.route("/transaction/<path:txn_id>/tag/<int:tag_id>/remove", methods=["POST"])
def remove_tag(txn_id, tag_id):
    """Remove a tag from a transaction."""
    g.db.execute("DELETE FROM transaction_tags WHERE transaction_id = ? AND tag_id = ?",
                 (txn_id, tag_id))
    g.db.commit()

    if request.headers.get("HX-Request"):
        tags = g.db.execute("""
            SELECT tg.* FROM tags tg
            JOIN transaction_tags tt ON tg.id = tt.tag_id
            WHERE tt.transaction_id = ?
        """, (txn_id,)).fetchall()
        all_tags = g.db.execute("SELECT name FROM tags ORDER BY name").fetchall()
        return render_template("partials/tags.html", txn_id=txn_id, tags=tags, all_tags=all_tags)
    return redirect(url_for("index"))


@app.route("/tags/create", methods=["POST"])
def create_tag():
    """Create a new tag."""
    name = request.form.get("name", "").strip()
    color = request.form.get("color", "#6b7280").strip()
    if name:
        g.db.execute("INSERT OR IGNORE INTO tags (name, color) VALUES (?, ?)", (name, color))
        g.db.commit()
    return redirect(url_for("tags_page"))


@app.route("/tags/<int:tag_id>/delete", methods=["POST"])
def delete_tag(tag_id):
    """Delete a tag."""
    g.db.execute("DELETE FROM transaction_tags WHERE tag_id = ?", (tag_id,))
    g.db.execute("DELETE FROM tags WHERE id = ?", (tag_id,))
    g.db.commit()
    return redirect(url_for("tags_page"))


@app.route("/transaction/<path:txn_id>/label", methods=["POST"])
def update_label(txn_id):
    """Set a human-readable label on a transaction."""
    label = request.form.get("label", "").strip() or None
    g.db.execute("UPDATE transactions SET label = ? WHERE id = ?", (label, txn_id))
    g.db.commit()

    txn = g.db.execute("SELECT * FROM transactions WHERE id = ?", (txn_id,)).fetchone()
    pattern = extract_pattern(txn["description"])
    matches = find_matching_transactions(g.db, pattern, exclude_id=txn_id)

    if request.headers.get("HX-Request"):
        return render_template("partials/label_form.html", txn=txn, pattern=pattern, match_count=len(matches))
    return redirect(url_for("index"))


@app.route("/transaction/<path:txn_id>/category", methods=["POST"])
def update_category(txn_id):
    """Set a category on a transaction."""
    category_name = request.form.get("category_name", "").strip()
    category_id = None
    if category_name:
        g.db.execute("INSERT OR IGNORE INTO categories (name) VALUES (?)", (category_name,))
        cat = g.db.execute("SELECT id FROM categories WHERE name = ?", (category_name,)).fetchone()
        category_id = cat["id"]

    g.db.execute("UPDATE transactions SET category_id = ? WHERE id = ?", (category_id, txn_id))
    g.db.commit()

    txn = g.db.execute("""
        SELECT t.*, c.name as category_name, c.color as category_color
        FROM transactions t
        LEFT JOIN categories c ON t.category_id = c.id
        WHERE t.id = ?
    """, (txn_id,)).fetchone()
    pattern = extract_pattern(txn["description"])
    matches = find_matching_transactions(g.db, pattern, exclude_id=txn_id)

    if request.headers.get("HX-Request"):
        return render_template("partials/category_form.html", txn=txn,
                               pattern=pattern, match_count=len(matches))
    return redirect(url_for("index"))


@app.route("/transaction/<path:txn_id>/apply-rule", methods=["POST"])
def apply_rule_from_transaction(txn_id):
    """Create a rule from this transaction and apply to all similar ones."""
    txn = g.db.execute("SELECT * FROM transactions WHERE id = ?", (txn_id,)).fetchone()
    pattern = request.form.get("pattern", extract_pattern(txn["description"])).strip()
    label = txn["label"]
    category_id = txn["category_id"]

    # Create or update the rule
    existing = g.db.execute("SELECT id FROM rules WHERE pattern = ?", (pattern,)).fetchone()
    if existing:
        g.db.execute("UPDATE rules SET label = ?, category_id = ? WHERE id = ?",
                     (label, category_id, existing["id"]))
    else:
        g.db.execute("INSERT INTO rules (pattern, label, category_id) VALUES (?, ?, ?)",
                     (pattern, label, category_id))

    # Apply to matching transactions
    matches = find_matching_transactions(g.db, pattern)
    for match_id in matches:
        updates = []
        params = []
        if label:
            updates.append("label = ?")
            params.append(label)
        if category_id:
            updates.append("category_id = ?")
            params.append(category_id)
        if updates:
            params.append(match_id)
            g.db.execute(f"UPDATE transactions SET {', '.join(updates)} WHERE id = ?", params)

    g.db.commit()

    if request.headers.get("HX-Request"):
        return f'<span style="color: var(--green); font-size: 0.8rem;">Applied to {len(matches)} transactions</span>'
    return redirect(url_for("index"))


@app.route("/categories")
def categories_page():
    """Category management page."""
    cats = g.db.execute("""
        SELECT c.*, COUNT(t.id) as txn_count,
               SUM(CASE WHEN CAST(t.amount AS REAL) < 0 THEN CAST(t.amount AS REAL) ELSE 0 END) as total_spent
        FROM categories c
        LEFT JOIN transactions t ON t.category_id = c.id
        GROUP BY c.id
        ORDER BY c.name
    """).fetchall()
    return render_template("categories.html", categories=cats)


@app.route("/categories/create", methods=["POST"])
def create_category():
    """Create a new category."""
    name = request.form.get("name", "").strip()
    color = request.form.get("color", "#6b7280").strip()
    if name:
        g.db.execute("INSERT OR IGNORE INTO categories (name, color) VALUES (?, ?)", (name, color))
        g.db.commit()
    return redirect(url_for("categories_page"))


@app.route("/categories/<int:cat_id>/color", methods=["POST"])
def update_category_color(cat_id):
    """Update a category's color."""
    color = request.form.get("color", "#6b7280").strip()
    g.db.execute("UPDATE categories SET color = ? WHERE id = ?", (color, cat_id))
    g.db.commit()
    return redirect(url_for("categories_page"))


@app.route("/categories/<int:cat_id>/delete", methods=["POST"])
def delete_category(cat_id):
    """Delete a category."""
    g.db.execute("UPDATE transactions SET category_id = NULL WHERE category_id = ?", (cat_id,))
    g.db.execute("DELETE FROM rules WHERE category_id = ?", (cat_id,))
    g.db.execute("DELETE FROM categories WHERE id = ?", (cat_id,))
    g.db.commit()
    return redirect(url_for("categories_page"))


@app.route("/rules")
def rules_page():
    """Rules management page."""
    rules = g.db.execute("""
        SELECT r.*, c.name as category_name
        FROM rules r
        LEFT JOIN categories c ON r.category_id = c.id
        ORDER BY r.created_at DESC
    """).fetchall()
    return render_template("rules.html", rules=rules)


@app.route("/rules/<int:rule_id>/delete", methods=["POST"])
def delete_rule(rule_id):
    """Delete a rule."""
    g.db.execute("DELETE FROM rules WHERE id = ?", (rule_id,))
    g.db.commit()
    return redirect(url_for("rules_page"))


@app.route("/rules/apply-all", methods=["POST"])
def apply_all():
    """Re-apply all rules to unlabeled/uncategorized transactions."""
    count = apply_all_rules(g.db)
    return redirect(url_for("rules_page"))


@app.route("/report")
def report():
    """Spending report grouped by tag and category."""
    by_tag = g.db.execute("""
        SELECT tg.name as tag_name, tg.color,
               COUNT(*) as count,
               SUM(CAST(t.amount AS REAL)) as total
        FROM transactions t
        JOIN transaction_tags tt ON t.id = tt.transaction_id
        JOIN tags tg ON tt.tag_id = tg.id
        WHERE t.amount < 0
        GROUP BY tg.id
        ORDER BY total ASC
    """).fetchall()

    by_category = g.db.execute("""
        SELECT c.name as category_name, c.color,
               COUNT(*) as count,
               SUM(CAST(t.amount AS REAL)) as total
        FROM transactions t
        JOIN categories c ON t.category_id = c.id
        WHERE t.amount < 0
        GROUP BY c.id
        ORDER BY total ASC
    """).fetchall()

    untagged = g.db.execute("""
        SELECT COUNT(*) as count, SUM(CAST(t.amount AS REAL)) as total
        FROM transactions t
        LEFT JOIN transaction_tags tt ON t.id = tt.transaction_id
        WHERE tt.transaction_id IS NULL AND t.amount < 0
    """).fetchone()

    uncategorized = g.db.execute("""
        SELECT COUNT(*) as count, SUM(CAST(t.amount AS REAL)) as total
        FROM transactions t
        WHERE t.category_id IS NULL AND t.amount < 0
    """).fetchone()

    return render_template("report.html", by_tag=by_tag, by_category=by_category,
                           untagged=untagged, uncategorized=uncategorized)


def format_date(epoch):
    """Template filter: format unix timestamp as human-readable date."""
    if not epoch:
        return "Pending"
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%b %d, %Y")


def format_amount(amount_str):
    """Template filter: format amount string as currency."""
    try:
        val = float(amount_str)
        if val < 0:
            return f"-${abs(val):,.2f}"
        return f"${val:,.2f}"
    except (ValueError, TypeError):
        return amount_str


app.jinja_env.filters["date"] = format_date
app.jinja_env.filters["amount"] = format_amount


if __name__ == "__main__":
    init_db()
    app.run(debug=True, port=5050)

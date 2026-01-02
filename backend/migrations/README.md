# Database Migrations

This directory contains all database schema migrations for the project.

Migrations are applied **manually or via script**, not automatically by the application.
The application never creates or modifies database schema at runtime.

---

## 📌 Migration Rules

1. **Migrations are immutable**
   - Once a migration has been applied to any environment, it must never be modified.
   - Changes require a new migration file.

2. **Migrations are applied in order**
   - Files are executed in lexicographical order.
   - Use numeric prefixes: `001_`, `002_`, `003_`, etc.

3. **Each migration must be idempotent**
   - Use `IF NOT EXISTS` where applicable.
   - Never assume a clean database.

4. **No destructive changes without a new migration**
   - Dropping tables or columns must be done explicitly in a new migration.

---

## 📁 Migration Structure

Example:

migrations/
├── 001_create_prices.sql
├── 002_add_price_indexes.sql
└── README.md

yaml
Copy code

---

## 🚀 Applying Migrations

Migrations are applied using the provided script:

```bash
python scripts/run_migrations.py
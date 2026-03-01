# SpreedSQL

**SQL-like declarative schema definitions for Google Sheets.**

SpreedSQL lets you define Google Sheets schemas the same way you define database schemas with SQL DDL, but for spreadsheets.

## Quick Start

```python
from spreed_sql import Spreadsheet, Tab, Column, DataType, OnEdit, Filter, execute_schema

schema = Spreadsheet(name_template="CRM_{name}")

schema.tab("Clientes",
    Column("Nombre", DataType.TEXT, required=True),
    Column("Email", DataType.EMAIL),
    Column("Estado", DataType.ENUM, values=["Activo", "Inactivo"]),
    Column("Monto", DataType.CURRENCY),
    triggers=[
        OnEdit(column="Estado", webhook_url="https://api.example.com/webhook"),
        OnSchedule(webhook_url="https://api.example.com/cron", cron_expression="0 9 * * *"),
    ],
    filter=Filter(enabled=True),
)

sheet_id, url, apps_script = execute_schema(schema, admin_email="admin@company.com", name="MiEmpresa")
```

## Features

- **Declarative schemas** — Define tabs, columns, types, and validations in Python
- **Auto-validation** — ENUM dropdowns, email/URL validation, required fields
- **Formulas & computed columns** — ARRAYFORMULA, auto-timestamps, calculated fields
- **Auto-filters & sorting** — Enable filters and default sorts per tab
- **Pivot tables** — Define pivot tables that auto-generate via QUERY()
- **Apps Script generation** — Auto-generate onEdit() triggers from schema
- **Schema validation** — Lint existing sheets against your schema definition
- **Format rules** — Conditional formatting (color cells by value)
- **Hidden columns** — Internal IDs invisible to end users

## Installation

```bash
pip install -e ../SpreedSQL
```

## API Reference

See `spreed_sql/models.py` for all available classes and options.

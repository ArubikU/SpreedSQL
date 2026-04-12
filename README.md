# SpreedSQL

**SQL-like declarative schema definitions for Google Sheets.**

SpreedSQL lets you define Google Sheets schemas the same way you define database schemas with SQL DDL, but for spreadsheets.

## Quick Start

```python
from spreed_sql import Spreadsheet, Tab, Column, DataType, OnEdit, Filter, execute_schema, TableTheme

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
    is_native_table=True,
    table_name="Tabla_Clientes",
    table_theme=TableTheme(),
)

sheet_id, url, apps_script = execute_schema(schema, admin_email="admin@company.com", name="MiEmpresa")
```

## ENUM dinámico (referencia entre tablas)

Puedes usar `DataType.ENUM` y en `values` pasar una referencia `"Pestaña.Columna"` para crear un dropdown dinámico (auto-actualizable cuando crece la tabla fuente):

```python
schema.tab("Especialidades",
    Column("Nombre", DataType.TEXT, required=True),
    is_native_table=True,
    table_name="Tabla_Especialidades",
)

schema.tab("Citas",
    Column("Especialidad", DataType.ENUM, values="Especialidades.Nombre"),
    Column("Doctor", DataType.ENUM, values="Doctores.Nombre", enum_allow_custom=False),
)
```

- `values=[...]` mantiene dropdown estático.
- `values="Tab.Columna"` crea dropdown por rango (`ONE_OF_RANGE`) con actualización automática.
- `enum_allow_custom=True` permite escribir fuera del dropdown cuando necesites edición flexible.

Internamente, SpreedSQL aplica estas validaciones con `spreadsheets.batchUpdate` + `setDataValidation` (JSON crudo de Google Sheets API), para soportar escenarios profesionales de CRM sin depender de clases limitadas de `gspread`.

## Metadata de tablas (TableID/range)

Si quieres persistir metadatos de tabla en tu backend (ej. Supabase), ejecuta con `return_metadata=True`:

```python
sheet_id, url, apps_script, metadata = execute_schema(
    schema,
    admin_email="admin@company.com",
    name="MiEmpresa",
    return_metadata=True,
)

print(metadata)
# {
#   "tables": [
#     {
#       "table_name": "Tabla_Clientes",
#       "sheet_name": "Clientes",
#       "sheet_id": 123456789,
#       "range_a1": "A1:D1000",
#       "row_end": 1000,
#       "col_end": 4
#     }
#   ]
# }
```

## Features

- **Declarative schemas** — Define tabs, columns, types, and validations in Python
- **Auto-validation** — ENUM dropdowns, email/URL validation, required fields
- **Formulas & computed columns** — ARRAYFORMULA, auto-timestamps, calculated fields
- **Auto-filters & sorting** — Enable filters and default sorts per tab
- **Native table mode** — Named range + filter + zebra banding (Google Sheets style)
- **Pivot tables** — Define pivot tables that auto-generate via QUERY()
- **Apps Script generation** — Auto-generate onEdit() triggers from schema
- **Schema validation** — Lint existing sheets against your schema definition
- **Format rules** — Conditional formatting (color cells by value)
- **Hidden columns** — Internal IDs invisible to end users

## Installation

```bash
pip install spreed-sql
```

## API Reference

See `spreed_sql/models.py` for all available classes and options.
